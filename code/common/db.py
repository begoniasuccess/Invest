import os
import re
import shutil
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
import pandas as pd
import psycopg
from psycopg.conninfo import conninfo_to_dict

from pathlib import Path
from dotenv import load_dotenv
from psycopg import sql as pg_sql
from common import utils

# PowerShell 範例：
# $env:POSTGRES_DSN = "host=localhost port=5432 dbname=invest user=postgres password=密碼"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
POSTGRES_DSN_ENV = "POSTGRES_DSN"
DEFAULT_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
DEFAULT_SEARCH_PATH = "report,core,raw_finmind,raw_exchange,ml,public"
POSTGRES_SEARCH_PATH = os.getenv("POSTGRES_SEARCH_PATH", DEFAULT_SEARCH_PATH)


def _validated_search_path() -> str:
    """回傳可安全套用至 PostgreSQL 連線的 schema 搜尋路徑。"""
    schemas = [item.strip() for item in POSTGRES_SEARCH_PATH.split(",") if item.strip()]
    if not schemas or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item) for item in schemas):
        raise RuntimeError(
            "POSTGRES_SEARCH_PATH 格式錯誤；請使用逗號分隔的 schema 名稱。"
        )
    return ",".join(schemas)


def _configure_connection(conn) -> None:
    """讓未加 schema 的既有 SQL 仍能在分層後正確解析。"""
    conn.execute(
        "SELECT set_config('search_path', %s, false)",
        (_validated_search_path(),),
    )


def _get_dsn() -> str:
    dsn = os.getenv(POSTGRES_DSN_ENV, "").strip()
    if not dsn:
        raise RuntimeError(
            f"尚未設定環境變數 {POSTGRES_DSN_ENV}。\n"
            "PowerShell 範例：\n"
            '$env:POSTGRES_DSN = "host=localhost port=5432 dbname=invest '
            'user=postgres password=登入密碼"'
        )
    return dsn


def _convert_qmark_placeholders(statement: str) -> str:
    statement = statement.replace("%", "%%")
    """將 SQL 中真正的 qmark 參數轉成 psycopg 的 %s。

    單引號、雙引號、行註解、區塊註解及 dollar-quoted 字串內的問號
    都會原樣保留。
    """
    result: list[str] = []
    i = 0
    length = len(statement)
    state = "normal"
    dollar_tag = ""

    while i < length:
        ch = statement[i]
        nxt = statement[i + 1] if i + 1 < length else ""

        if state == "normal":
            if ch == "'":
                state = "single"
                result.append(ch)
            elif ch == '"':
                state = "double"
                result.append(ch)
            elif ch == "-" and nxt == "-":
                state = "line_comment"
                result.extend((ch, nxt))
                i += 1
            elif ch == "/" and nxt == "*":
                state = "block_comment"
                result.extend((ch, nxt))
                i += 1
            elif ch == "$":
                match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", statement[i:])
                if match:
                    dollar_tag = match.group(0)
                    state = "dollar"
                    result.append(dollar_tag)
                    i += len(dollar_tag) - 1
                else:
                    result.append(ch)
            elif ch == "?":
                result.append("%s")
            else:
                result.append(ch)

        elif state == "single":
            result.append(ch)
            if ch == "'":
                if nxt == "'":
                    result.append(nxt)
                    i += 1
                else:
                    state = "normal"

        elif state == "double":
            result.append(ch)
            if ch == '"':
                if nxt == '"':
                    result.append(nxt)
                    i += 1
                else:
                    state = "normal"

        elif state == "line_comment":
            result.append(ch)
            if ch in "\r\n":
                state = "normal"

        elif state == "block_comment":
            result.append(ch)
            if ch == "*" and nxt == "/":
                result.append(nxt)
                i += 1
                state = "normal"

        elif state == "dollar":
            if statement.startswith(dollar_tag, i):
                result.append(dollar_tag)
                i += len(dollar_tag) - 1
                state = "normal"
            else:
                result.append(ch)

        i += 1

    return "".join(result)


def _params_tuple(params):
    if params is None:
        return ()
    if isinstance(params, tuple):
        return tuple(_adapt_value(value) for value in params)
    if isinstance(params, list):
        return tuple(_adapt_value(value) for value in params)
    return params


def _adapt_value(value):
    """將 Pandas／NumPy scalar 轉成 psycopg 可安全寫入的 Python 值。"""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


@contextmanager
def get_connection():
    """建立並自動關閉 PostgreSQL 連線。

    呼叫端若直接使用 conn.execute()，請使用 PostgreSQL 的 %s 參數格式；
    db.py 對外查詢／寫入函式則仍可接受原本的 ? 格式。
    """
    conn = psycopg.connect(_get_dsn())
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()


def query_to_df(sql: str, params: tuple | list | None = ()) -> pd.DataFrame:
    """執行查詢並回傳 DataFrame；相容原本的 ? 參數占位符。"""
    return sql_to_df(sql, params=params)


def sql_to_df(
    sql: str,
    params: tuple | list | None = None,
    parse_dates: list[str] | None = None,
    dtype: dict | None = None,
) -> pd.DataFrame:
    """傳入 SQL 並回傳 DataFrame。"""
    statement = _convert_qmark_placeholders(sql)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, _params_tuple(params))
            if cur.description is None:
                return pd.DataFrame()
            columns = [column.name for column in cur.description]
            df = pd.DataFrame.from_records(cur.fetchall(), columns=columns)

    if parse_dates:
        for column in parse_dates:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], errors="coerce")

    if dtype:
        applicable = {key: value for key, value in dtype.items() if key in df.columns}
        if applicable:
            df = df.astype(applicable)

    return df


def query_single_value(sql: str, params: tuple | list | None = ()):
    """執行查詢並回傳第一列第一欄；無資料時回傳 None。"""
    statement = _convert_qmark_placeholders(sql)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, _params_tuple(params))
            row = cur.fetchone()
            return row[0] if row else None


def execute_sql(sql: str, params: tuple | list[tuple] | None = ()) -> bool:
    """執行 INSERT／UPDATE／DELETE／DDL。

    tuple 代表單筆；list[tuple] 代表批次。成功回傳 True，失敗回傳 False。
    """
    statement = _convert_qmark_placeholders(sql)
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                if isinstance(params, list) and params and all(
                    isinstance(row, (tuple, list)) for row in params
                ):
                    cur.executemany(statement, [_params_tuple(row) for row in params])
                else:
                    cur.execute(statement, _params_tuple(params))
            conn.commit()
        return True
    except psycopg.Error as exc:
        print(f"[PostgreSQL Error] {exc}")
        return False


def execute_sql_autocommit(sql: str, params: tuple | None = ()) -> None:
    """在 autocommit 模式執行單一 SQL。

    `REFRESH MATERIALIZED VIEW CONCURRENTLY` 不可位於一般交易區塊內，
    因此報表快取刷新使用此函式。失敗時保留例外，交由呼叫端處理。
    """
    statement = _convert_qmark_placeholders(sql)
    with psycopg.connect(_get_dsn(), autocommit=True) as conn:
        _configure_connection(conn)
        with conn.cursor() as cur:
            cur.execute(statement, _params_tuple(params))


def copy_query_to_csv(
    query: str,
    output_path: str | Path,
    *,
    include_utf8_bom: bool = True,
) -> int:
    """以 PostgreSQL COPY 串流輸出查詢結果，避免 `fetchall()` 佔用大量記憶體。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    copy_sql = f"COPY ({query.rstrip().rstrip(';')}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"

    with psycopg.connect(_get_dsn()) as conn:
        _configure_connection(conn)
        with conn.cursor() as cur:
            with path.open("wb") as output:
                if include_utf8_bom:
                    output.write(b"\xef\xbb\xbf")
                with cur.copy(copy_sql) as copy:
                    for block in copy:
                        output.write(bytes(block))
            row_count = max(int(cur.rowcount or 0), 0)
    return row_count


def _relation_info(conn, table_name: str, schema: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            UNION ALL
            SELECT 'VIEW'
            FROM information_schema.views
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
            """,
            (schema, table_name, schema, table_name),
        )
        row = cur.fetchone()
        if not row:
            return None, []

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
            WHERE n.nspname = %s AND c.relname = %s AND i.indisprimary
            ORDER BY k.ord
            """,
            (schema, table_name),
        )
        return row[0], [item[0] for item in cur.fetchall()]


def export_table_to_csv(
    table_name: str,
    batch_size: int = 2000,
    output_dir: str = "C:/Users/USER/Desktop",
    verbose: bool = True,
    schema: str = DEFAULT_SCHEMA,
):
    """將 PostgreSQL table 或 view 分批匯出為 UTF-8 BOM CSV。"""
    if batch_size <= 0:
        raise ValueError("batch_size 必須大於 0")

    t_start = time.time()
    batch_no = 1
    offset = 0
    total_rows = 0
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(
        output_dir,
        f"{table_name}_export_{datetime.today().strftime('%Y%m%d_%H%M%S')}.csv",
    )

    if verbose:
        print(f"🚀 開始匯出表：{schema}.{table_name}")
        print(f"📁 輸出檔案：{out_file}")

    with get_connection() as conn:
        relation_type, pk_cols = _relation_info(conn, table_name, schema)
    if relation_type is None:
        raise ValueError(f"❌ 找不到 {schema}.{table_name}（table/view 不存在）")

    while True:
        order_clause = pg_sql.SQL("")
        if pk_cols:
            order_clause = pg_sql.SQL(" ORDER BY {} ").format(
                pg_sql.SQL(", ").join(map(pg_sql.Identifier, pk_cols))
            )

        statement = pg_sql.SQL("SELECT * FROM {}.{}{} LIMIT %s OFFSET %s").format(
            pg_sql.Identifier(schema),
            pg_sql.Identifier(table_name),
            order_clause,
        )

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(statement, (batch_size, offset))
                columns = [column.name for column in cur.description]
                df = pd.DataFrame.from_records(cur.fetchall(), columns=columns)

        if df.empty:
            if verbose:
                print("✅ 已無更多資料，停止分批")
            break

        df.to_csv(
            out_file,
            index=False,
            encoding="utf-8-sig",
            mode="w" if batch_no == 1 else "a",
            header=batch_no == 1,
        )
        row_count = len(df)
        total_rows += row_count
        if verbose:
            print(f"📦 批次 {batch_no}: {row_count} 筆（OFFSET={offset}）")
        offset += row_count
        batch_no += 1

    total_time = time.time() - t_start
    print(f"🎯 匯出完成，共 {batch_no - 1} 批，{total_rows} 筆，耗時 {total_time:.2f}s")
    print(f"📄 檔案路徑：{out_file}")
    return out_file


def export_database_schema(
    output_dir: str = "C:/Users/USER/Desktop",
    output_filename: str | None = None,
    schema: str | None = None,
    pg_dump_path: str | None = None,
    verbose: bool = True,
) -> str:
    """將目前 PostgreSQL 資料庫的結構匯出成 SQL，不包含資料。

    連線資訊沿用環境變數 POSTGRES_DSN。

    Args:
        output_dir: SQL 檔案輸出資料夾。
        output_filename: 自訂檔名；省略時自動使用資料庫名稱與時間。
        schema: 指定要匯出的 schema，例如 ``public``；None 代表整個資料庫。
        pg_dump_path: pg_dump.exe 的完整路徑；省略時自動尋找。
        verbose: 是否顯示執行訊息。

    Returns:
        匯出完成的 SQL 檔案完整路徑。
    """
    dsn_config = conninfo_to_dict(_get_dsn())
    database = dsn_config.get("dbname")
    if not database:
        raise RuntimeError("POSTGRES_DSN 缺少 dbname，無法判斷要匯出的資料庫。")

    if pg_dump_path is None:
        pg_dump_path = shutil.which("pg_dump")
        if pg_dump_path is None:
            default_path = Path("C:/Program Files/PostgreSQL/18/bin/pg_dump.exe")
            if default_path.is_file():
                pg_dump_path = str(default_path)

    if not pg_dump_path or not Path(pg_dump_path).is_file():
        raise FileNotFoundError(
            "找不到 pg_dump。請確認 PostgreSQL 18 已安裝，或透過 "
            "pg_dump_path 傳入 pg_dump.exe 的完整路徑。"
        )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    if output_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{database}_schema_{timestamp}.sql"
    elif not output_filename.lower().endswith(".sql"):
        output_filename += ".sql"
    destination = (output_path / output_filename).resolve()

    command = [
        str(pg_dump_path),
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(destination),
        "--dbname",
        database,
    ]

    option_map = {
        "host": "--host",
        "port": "--port",
        "user": "--username",
    }
    for dsn_key, option in option_map.items():
        value = dsn_config.get(dsn_key)
        if value:
            command.extend((option, value))

    if schema:
        command.extend(("--schema", schema))

    process_env = os.environ.copy()
    password = dsn_config.get("password")
    if password:
        process_env["PGPASSWORD"] = password

    if verbose:
        scope = f"schema={schema}" if schema else "整個資料庫"
        print(f"🚀 開始匯出 PostgreSQL 結構：{database}（{scope}）")
        print(f"📁 輸出檔案：{destination}")

    try:
        subprocess.run(
            command,
            env=process_env,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.CalledProcessError as exc:
        destination.unlink(missing_ok=True)
        message = (exc.stderr or exc.stdout or str(exc)).strip()
        raise RuntimeError(f"pg_dump 匯出失敗：{message}") from exc

    if not destination.is_file() or destination.stat().st_size == 0:
        raise RuntimeError("pg_dump 執行完成，但輸出的 schema SQL 檔案不存在或為空。")

    if verbose:
        print(f"✅ Schema 匯出完成：{destination}")
    return str(destination)


def _limit_date_span(base_date: datetime = None, where_condition: str = None):
    """將 date_span.end_date 限制在 base_date - 2 days。"""
    if base_date is None:
        base_date = datetime.now()
    target_day = (base_date - timedelta(days=2)).date()
    statement = """
        UPDATE date_span
        SET end_date = ?
        WHERE end_date > ?
    """
    if where_condition is not None and str(where_condition).strip():
        statement += f" {where_condition}"
    return execute_sql(statement, (target_day, target_day))


def _get_date_span_record(target_table: str, idx_key: str):
    df = query_to_df(
        """
        SELECT start_date, end_date
        FROM date_span
        WHERE target_table = ? AND idx_key = ?
        LIMIT 1
        """,
        (target_table, str(idx_key)),
    )
    if df.empty:
        return None, None
    start = pd.to_datetime(df.loc[0, "start_date"], errors="coerce")
    end = pd.to_datetime(df.loc[0, "end_date"], errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return None, None
    return start.normalize(), end.normalize()


def _span_covers_request(span_s, span_e, req_s: pd.Timestamp, req_e: pd.Timestamp) -> bool:
    if span_s is None or span_e is None:
        return False
    return (
        pd.Timestamp(span_s).normalize() <= req_s
        and pd.Timestamp(span_e).normalize() >= req_e
    )


def _subtract_covered_ranges(req_s, req_e, covered_spans):
    req_s = pd.Timestamp(req_s).normalize()
    req_e = pd.Timestamp(req_e).normalize()
    if req_s > req_e:
        return []

    cleaned = []
    for start, end in covered_spans:
        if start is None or end is None:
            continue
        start = pd.Timestamp(start).normalize()
        end = pd.Timestamp(end).normalize()
        if end < req_s or start > req_e:
            continue
        cleaned.append((max(start, req_s), min(end, req_e)))

    if not cleaned:
        return [(req_s, req_e)]

    cleaned.sort(key=lambda item: item[0])
    merged = []
    for start, end in cleaned:
        if not merged or start > merged[-1][1] + pd.Timedelta(days=1):
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    missing = []
    cursor = req_s
    for start, end in merged:
        if cursor < start:
            missing.append((cursor, start - pd.Timedelta(days=1)))
        cursor = max(cursor, end + pd.Timedelta(days=1))
    if cursor <= req_e:
        missing.append((cursor, req_e))
    return missing


def _upsert_date_span_merge(target_table, idx_key, new_s, new_e):
    idx_key = str(idx_key)
    new_s = pd.Timestamp(new_s).normalize()
    new_e = utils._cap_end_date_to_yesterday(pd.Timestamp(new_e).normalize())
    if new_s > new_e:
        utils.ptMsg(
            f"⚠️ date_span 不更新：{target_table}[{idx_key}] "
            f"new_s={utils._dstr(new_s)}, new_e已被限制到昨天={utils._dstr(new_e)}"
        )
        return

    old_s, old_e = _get_date_span_record(target_table, idx_key)
    if old_s is None or old_e is None:
        final_s, final_e = new_s, new_e
    else:
        old_e = utils._cap_end_date_to_yesterday(pd.Timestamp(old_e).normalize())
        final_s = min(pd.Timestamp(old_s).normalize(), new_s)
        final_e = max(old_e, new_e)
    final_e = utils._cap_end_date_to_yesterday(final_e)

    if final_s > final_e:
        utils.ptMsg(
            f"⚠️ date_span 不更新：{target_table}[{idx_key}] "
            f"final_s={utils._dstr(final_s)}, final_e={utils._dstr(final_e)}"
        )
        return

    execute_sql(
        """
        INSERT INTO date_span
            (target_table, idx_key, start_date, end_date, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (target_table, idx_key) DO UPDATE SET
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = CURRENT_TIMESTAMP
        """,
        [(
            target_table,
            idx_key,
            final_s.date(),
            final_e.date(),
        )],
    )
    utils.ptMsg(
        f"📌 date_span 已更新：{target_table}[{idx_key}] "
        f"{final_s.strftime('%Y-%m-%d')}~{final_e.strftime('%Y-%m-%d')}"
    )


def _delete_stock_spans_fully_covered_by_all(target_table: str):
    all_s, all_e = _get_date_span_record(target_table, "all")
    if all_s is None or all_e is None:
        return
    execute_sql(
        """
        DELETE FROM date_span
        WHERE target_table = ?
          AND idx_key != 'all'
          AND start_date >= ?
          AND end_date <= ?
        """,
        [(
            target_table,
            all_s.date(),
            all_e.date(),
        )],
    )
    utils.ptMsg(
        f"🧹 date_span 已刪除被 all 完全覆蓋的個股快取紀錄："
        f"{target_table} all={all_s.strftime('%Y-%m-%d')}~{all_e.strftime('%Y-%m-%d')}"
    )


# python -m common.db
if __name__ == "__main__":
    print("====== run python -m common.db =========")
    export_database_schema(
        output_dir="C:/Users/USER/Desktop/Han/學習/金融策略/Invest",
        output_filename="postgres_schema.sql",
        schema=None,
    )

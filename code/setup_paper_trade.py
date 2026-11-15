"""Install and verify the paper trade schema against the Dashboard database."""
from pathlib import Path
from common import db

REQUIRED = {"orders": {"position_effect", "time_in_force", "cancel_reason", "trigger_direction"},
            "trades": {"position_quantity", "exit_order_id"},
            "journals": {"id", "title", "body", "journal_date", "updated_at", "tags"},
            "journal_links": {"journal_id", "account_id", "run_id", "trade_id", "historical_date", "entry_order_id"}}


def main():
    try:
        import markdown_it  # noqa: F401
        import PIL  # noqa: F401
    except ImportError as exc:
        raise RuntimeError('Install Markdown dependency: python -m pip install "markdown-it-py>=3.0,<5" "Pillow>=11.3,<13"') from exc
    sql = (Path(__file__).resolve().parent / "dashboard" / "sql" / "paper_trade.sql").read_text(encoding="utf-8")
    with db.get_connection() as conn:
        conn.commit()
        conn.autocommit = True
        print("Database:", conn.execute("SELECT current_database()").fetchone()[0])
        try:
            conn.execute(sql, prepare=False)
        except Exception:
            conn.execute("ROLLBACK")
            raise
        rows = conn.execute("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'paper_trade'").fetchall()
        found = {(table, column) for table, column in rows}
        missing = sorted((table, column) for table, columns in REQUIRED.items() for column in columns if (table, column) not in found)
        if missing:
            raise RuntimeError(f"Missing schema columns: {missing}")
        legacy = [(t,c) for t,c in found if t in ("runs","trades") and c in ("note","note_updated_at")]
        if legacy: raise RuntimeError(f"Legacy note columns remain: {legacy}")
        # A unique constraint on order_id would prevent partial position splits.
        old_unique = conn.execute("""SELECT 1 FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1] WHERE c.conrelid='paper_trade.trades'::regclass AND c.contype='u' AND cardinality(c.conkey)=1 AND a.attname='order_id'""").fetchone()
        if old_unique:
            raise RuntimeError("Legacy UNIQUE(order_id) remains; partial closing cannot work.")
        conn.execute("SELECT COALESCE(t.position_quantity,o.quantity), t.exit_order_id, o.position_effect, o.time_in_force, o.cancel_reason, o.trigger_direction FROM paper_trade.trades t JOIN paper_trade.orders o ON o.id=t.order_id LIMIT 0")
        conn.execute("SELECT entry_order_id FROM paper_trade.journal_link_details LIMIT 0")
        print("Verified: position fields, time_in_force, trigger_direction, journal opening-order links.")
        print("paper_trade schema ready. Existing market and backtest tables were not changed.")


if __name__ == "__main__":
    main()
    from dashboard.journal_media import media_root
    print("Journal media:", media_root())

from __future__ import annotations


FUTURES_CHIP_POSITION_COLUMNS = (
    "日法人多倉",
    "日法人空倉",
    "夜法人多單量",
    "夜法人空單量",
    "未沖銷_全",
    "未沖銷_近期",
    "未沖銷_遠期",
    "主力_月結算狀態",
    "主力新建多單",
    "主力新建空單",
    "主力淨多空和_5日",
    "主力淨多空和_10日",
    "主力淨多空和_20日",
    "主力淨多空和_60日",
)

TAIEX_CORE_COLUMNS = (
    "策略_核心_昨價量",
    "策略_核心_五日價量",
)

FUTURES_CORE_COLUMNS = (
    "策略_核心_昨價量",
    "策略_核心_五日價量",
)


def order_futures_report_columns(columns: list[str] | tuple[str, ...]) -> list[str]:
    """將指定籌碼欄位依固定順序移到「夜布林下軌」之後。"""
    # 舊「策略_核心」已自正式期貨 View 移除；這裡仍防禦性排除，避免
    # 尚未執行遷移的資料庫把舊欄位輸出到 Dashboard／CSV。
    ordered = [column for column in columns if column != "策略_核心"]
    turning = [
        column
        for column in (
            "日盤走勢",
            "日盤轉折支撐",
            "日盤轉折壓力",
            "夜盤走勢",
            "夜盤轉折支撐",
            "夜盤轉折壓力",
        )
        if column in ordered
    ]
    ordered = [column for column in ordered if column not in turning]
    anchor = ordered.index("買賣權比_週五") + 1 if "買賣權比_週五" in ordered else len(ordered)
    ordered[anchor:anchor] = turning
    moving = [column for column in FUTURES_CHIP_POSITION_COLUMNS if column in ordered]
    if moving:
        ordered = [column for column in ordered if column not in moving]
        try:
            insert_at = ordered.index("夜布林下軌") + 1
        except ValueError:
            insert_at = len(ordered)
        ordered = ordered[:insert_at] + moving + ordered[insert_at:]

    core_columns = [column for column in FUTURES_CORE_COLUMNS if column in ordered]
    if not core_columns:
        return ordered
    ordered = [column for column in ordered if column not in core_columns]
    try:
        insert_at = ordered.index("建立時間")
    except ValueError:
        insert_at = len(ordered)
    return ordered[:insert_at] + core_columns + ordered[insert_at:]


def order_taiex_report_columns(columns: list[str] | tuple[str, ...]) -> list[str]:
    """顯示兩套明確核心欄位，資料庫舊欄位僅保留給既有程式相容。"""
    has_turnover_alias = "成交金額" in columns
    ordered = [
        "成交股數" if column == "成交量" else
        "成交金額" if column == "總成交金額_億" and not has_turnover_alias else
        column
        for column in columns if column != "策略_核心"
    ]
    if "成交股數" in ordered and "成交金額" in ordered:
        ordered.remove("成交金額")
        ordered.insert(ordered.index("成交股數") + 1, "成交金額")
    if "總成交金額_億" in ordered:
        ordered.remove("總成交金額_億")
        try:
            insert_at = ordered.index("布林下軌") + 1
        except ValueError:
            insert_at = len(ordered)
        ordered.insert(insert_at, "總成交金額_億")
    moving = [column for column in TAIEX_CORE_COLUMNS if column in ordered]
    if not moving:
        return ordered

    ordered = [column for column in ordered if column not in moving]
    try:
        insert_at = ordered.index("策略_線型_籌碼") + 1
    except ValueError:
        return ordered + moving
    return ordered[:insert_at] + moving + ordered[insert_at:]

from common.report_columns import (
    FUTURES_CHIP_POSITION_COLUMNS,
    order_futures_report_columns,
)


def test_moves_chip_columns_after_night_bollinger_lower_band():
    original = [
        "日期",
        "日法人多倉",
        "未沖銷_全",
        "夜布林下軌",
        "夜法人空單量",
        "散戶多空比_小台",
    ]

    result = order_futures_report_columns(original)

    assert result == [
        "日期",
        "夜布林下軌",
        "日法人多倉",
        "夜法人空單量",
        "未沖銷_全",
        "散戶多空比_小台",
    ]


def test_requested_order_is_stable_when_all_columns_exist():
    original = ["日期", *reversed(FUTURES_CHIP_POSITION_COLUMNS), "夜布林下軌", "尾欄"]
    result = order_futures_report_columns(original)
    anchor = result.index("夜布林下軌") + 1
    assert result[anchor : anchor + len(FUTURES_CHIP_POSITION_COLUMNS)] == list(
        FUTURES_CHIP_POSITION_COLUMNS
    )

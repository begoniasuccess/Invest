"""一次性建立加權報表 Dashboard 永久快取。

可選擇執行：python -m main.init_taiex_dashboard_cache
若不手動執行，第一次從 Dashboard 更新加權報表時也會自動建立。
"""
from module.taiex_turnover_rule import ensure_turnover_view
from module.taiex_report_cache import initialize_taiex_dashboard_cache


def main() -> None:
    ensure_turnover_view()
    rows = initialize_taiex_dashboard_cache()
    print(f"[加權快取] 初始化完成：{rows:,} 筆")


if __name__ == "__main__":
    main()

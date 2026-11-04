"""One-time historical rebuild after fixing TX minute-bar contract selection."""

from module.futures_turning import refresh_turning_levels


def main():
    print('[TX分K] 開始重建歷史五分K與日/夜盤轉折、走勢…')
    sessions = refresh_turning_levels(full=True)
    print(f'[TX分K] 完成：{sessions:,} 個時段。')
    print('[TX分K] Dashboard 分K圖已直接使用新選約規則，不需要另建圖表快取。')


if __name__ == '__main__':
    main()

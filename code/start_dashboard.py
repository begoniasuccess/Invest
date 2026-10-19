from __future__ import annotations

import os
import socket
import threading
import webbrowser

import uvicorn


def main() -> None:
    host = "127.0.0.1"
    port = int(os.getenv("DASHBOARD_PORT", "8050"))
    url = f"http://{host}:{port}"

    # 桌面捷徑被重複雙擊時，直接開啟既有 Dashboard，不再啟動第二個服務。
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.4)
        if probe.connect_ex((host, port)) == 0:
            print(f"Dashboard 已在執行，正在開啟：{url}")
            webbrowser.open(url)
            return

    threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    print(f"級距統計 Dashboard 已啟動：{url}")
    print("關閉此視窗即可停止 Dashboard。")
    uvicorn.run("dashboard.app:app", host=host, port=port, reload=False, log_level="info")


if __name__ == "__main__":
    main()

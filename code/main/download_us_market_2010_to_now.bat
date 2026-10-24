@echo off
setlocal
cd /d "%~dp0\.."
py main\download_us_market_daily.py --start 2010-01-01 --preset core
pause

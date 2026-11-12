@echo off
cd /d "%~dp0"
python setup_paper_trade.py
if errorlevel 1 (
  echo Installation failed. Check PostgreSQL connection and schema permissions.
) else (
  echo Installation completed. Restart Dashboard.
)
pause

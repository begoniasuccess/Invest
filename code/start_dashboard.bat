@echo off
cd /d "%~dp0"
python start_dashboard.py
if errorlevel 1 (
  echo.
  echo Dashboard failed to start. Run install_dashboard.bat first and check POSTGRES_DSN in .env.
  pause
)

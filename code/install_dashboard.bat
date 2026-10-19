@echo off
cd /d "%~dp0"
python -m pip install -r requirements-dashboard.txt
if errorlevel 1 (
  echo.
  echo Dashboard package installation failed.
  pause
  exit /b 1
)
echo.
echo Dashboard packages installed successfully.
pause

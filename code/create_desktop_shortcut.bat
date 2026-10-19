@echo off
setlocal
cd /d "%~dp0"

set "INVEST_DASHBOARD_TARGET=%~dp0start_dashboard.bat"
set "INVEST_DASHBOARD_WORKDIR=%~dp0"
set "INVEST_DASHBOARD_ICON=%~dp0dashboard\static\dashboard.ico"

if not exist "%INVEST_DASHBOARD_TARGET%" (
  echo start_dashboard.bat was not found.
  pause
  exit /b 1
)

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "$desktop=[Environment]::GetFolderPath('Desktop');" ^
  "$shell=New-Object -ComObject WScript.Shell;" ^
  "$shortcut=$shell.CreateShortcut((Join-Path $desktop 'Invest Dashboard.lnk'));" ^
  "$shortcut.TargetPath=$env:ComSpec;" ^
  "$shortcut.Arguments='/c ""' + $env:INVEST_DASHBOARD_TARGET + '""';" ^
  "$shortcut.WorkingDirectory=$env:INVEST_DASHBOARD_WORKDIR;" ^
  "$shortcut.Description='Start Invest Dashboard';" ^
  "if (Test-Path $env:INVEST_DASHBOARD_ICON) {$shortcut.IconLocation=$env:INVEST_DASHBOARD_ICON + ',0'};" ^
  "$shortcut.WindowStyle=1;" ^
  "$shortcut.Save()"

if errorlevel 1 (
  echo Failed to create the desktop shortcut.
  pause
  exit /b 1
)

echo Invest Dashboard shortcut was created on your desktop.
pause

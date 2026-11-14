@echo off
cd /d "%~dp0"
python -c "import markdown_it; import mdurl" >nul 2>&1
if errorlevel 1 (
  echo Installing Markdown preview dependency...
  python -m pip install "markdown-it-py>=3.0,<5"
  if errorlevel 1 goto failed
)
python setup_paper_trade.py
if errorlevel 1 goto failed
echo Installation completed. Restart Dashboard.
pause
exit /b 0
:failed
echo Installation failed. Do not restart until the error is resolved.
pause
exit /b 1

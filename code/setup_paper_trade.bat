@echo off
cd /d "%~dp0"
python -c "import markdown_it; import mdurl; import PIL; import openpyxl; assert int(PIL.__version__.split(chr(46))[0]) >= 11" >nul 2>&1
if errorlevel 1 (
  echo Installing Markdown and media validation dependencies...
  python -m pip install "markdown-it-py>=3.0,<5" "Pillow>=11.3,<13" "openpyxl>=3.1,<4"
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

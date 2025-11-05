@echo off
REM Navigate to project root
cd /d "%~dp0"

REM Run the parser
python src\main.py

REM Pause to show output
pause

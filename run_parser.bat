@echo off
REM Greyhound Race Form Parser - Quick Start
REM This batch file runs the enhanced parser with validation

echo ====================================
echo Greyhound Race Form Parser
echo ====================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)

echo Python found! Checking dependencies...
echo.

REM Check if required packages are installed
python -c "import pandas, pdfplumber, openpyxl" >nul 2>&1
if errorlevel 1 (
    echo Installing required dependencies...
    echo.
    pip install -r requirements.txt
    if errorlevel 1 (
        echo ERROR: Failed to install dependencies
        echo Please run: pip install -r requirements.txt
        echo.
        pause
        exit /b 1
    )
    echo.
    echo Dependencies installed successfully!
    echo.
)

REM Create outputs directory if it doesn't exist
if not exist "outputs" mkdir outputs

REM Create data directory if it doesn't exist
if not exist "data" mkdir data

echo Starting parser...
echo.
echo Place your PDF files in the /data folder before running.
echo.
echo Running main_enhanced.py...
echo ====================================
echo.

REM Run the enhanced parser
python main_enhanced.py

REM Check if the parser ran successfully
if errorlevel 1 (
    echo.
    echo ====================================
    echo ERROR: Parser failed to run
    echo Check the error messages above
    echo ====================================
    echo.
) else (
    echo.
    echo ====================================
    echo Parser completed successfully!
    echo.
    echo Output files are in the /outputs folder:
    echo   - todays_form_YYYYMMDD_HHMMSS.csv
    echo   - todays_form_YYYYMMDD_HHMMSS.xlsx
    echo ====================================
    echo.
)

pause

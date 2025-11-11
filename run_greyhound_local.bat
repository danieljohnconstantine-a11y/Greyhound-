@echo off
REM ====================================================================
REM Greyhound Analytics - Windows Local Execution Script
REM ====================================================================
REM This script sets up the environment and runs the greyhound analytics
REM pipeline end-to-end.
REM ====================================================================

cd /d "%~dp0"

echo.
echo ====================================================================
echo   Greyhound Analytics - Local Execution
echo ====================================================================
echo.

REM Create outputs directory if it doesn't exist
if not exist "outputs" (
    echo [INFO] Creating outputs directory...
    mkdir outputs
)

REM Create data directories if they don't exist
if not exist "data" mkdir data
if not exist "data\rns" mkdir data\rns
if not exist "data\combined" mkdir data\combined
if not exist "forms" mkdir forms
if not exist "reports" mkdir reports

REM Initialize log file
set LOG_FILE=run_log.txt
echo [%DATE% %TIME%] Greyhound Analytics Execution Started > %LOG_FILE%
echo [%DATE% %TIME%] Greyhound Analytics Execution Started

REM Check if Python is installed
echo [INFO] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH!
    echo [ERROR] Please install Python 3.8 or higher from https://www.python.org/downloads/
    echo [ERROR] Make sure to check "Add Python to PATH" during installation.
    echo.
    pause
    exit /b 1
)

python --version
python --version >> %LOG_FILE% 2>&1

REM Check Python version (basic check)
echo [INFO] Verifying Python version is suitable...
python -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)" 2>nul
if errorlevel 1 (
    echo [WARNING] Python version may be too old. Python 3.8+ is recommended.
    echo [WARNING] Current version:
    python --version
    echo.
)

REM Check if virtual environment exists, create if not
if not exist "venv" (
    echo [INFO] Creating Python virtual environment...
    echo [%DATE% %TIME%] Creating virtual environment >> %LOG_FILE%
    python -m venv venv >> %LOG_FILE% 2>&1
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment!
        echo [ERROR] Check %LOG_FILE% for details.
        pause
        exit /b 1
    )
    echo [SUCCESS] Virtual environment created.
) else (
    echo [INFO] Virtual environment already exists.
)

REM Activate virtual environment
echo [INFO] Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment!
    echo [ERROR] Please delete the 'venv' folder and run this script again.
    pause
    exit /b 1
)
echo [SUCCESS] Virtual environment activated.

REM Upgrade pip
echo [INFO] Upgrading pip...
echo [%DATE% %TIME%] Upgrading pip >> %LOG_FILE%
python -m pip install --upgrade pip >> %LOG_FILE% 2>&1

REM Install dependencies
echo [INFO] Installing required dependencies...
echo [INFO] This may take a few minutes on first run...
echo [%DATE% %TIME%] Installing dependencies from requirements.txt >> %LOG_FILE%

REM Try to install with verbose output to catch errors
pip install -r requirements.txt >> %LOG_FILE% 2>&1
set INSTALL_EXIT_CODE=%errorlevel%

if %INSTALL_EXIT_CODE% neq 0 (
    echo.
    echo [WARNING] Standard installation encountered issues.
    echo [INFO] Attempting installation with individual packages...
    echo [%DATE% %TIME%] Retrying individual package installation >> %LOG_FILE%
    
    REM Try installing packages one by one for better diagnostics
    pip install requests==2.32.3 >> %LOG_FILE% 2>&1
    pip install beautifulsoup4==4.12.3 >> %LOG_FILE% 2>&1
    pip install lxml==5.2.2 >> %LOG_FILE% 2>&1
    pip install pandas==2.2.2 >> %LOG_FILE% 2>&1
    pip install python-dateutil==2.9.0.post0 >> %LOG_FILE% 2>&1
    pip install pytz==2024.1 >> %LOG_FILE% 2>&1
    pip install tenacity==8.5.0 >> %LOG_FILE% 2>&1
    pip install openpyxl==3.1.5 >> %LOG_FILE% 2>&1
    
    echo.
    echo [INFO] Individual package installation attempted.
    echo [INFO] Some packages may have warnings but could still work.
    echo.
    echo Press any key to continue with pipeline execution...
    pause >nul
) else (
    echo [SUCCESS] Dependencies installed.
)

echo.
echo ====================================================================
echo   Running Greyhound Analytics Pipeline
echo ====================================================================
echo.

REM Run the validation script first as a test
echo [INFO] Running validation test...
echo [%DATE% %TIME%] Running validation test >> %LOG_FILE%
python validate_speed_implementation.py >> %LOG_FILE% 2>&1
if errorlevel 1 (
    echo [WARNING] Validation test had issues. Check %LOG_FILE% for details.
) else (
    echo [SUCCESS] Validation test passed.
)

echo.
echo [INFO] Running main pipeline (run_daily.py)...
echo [%DATE% %TIME%] Running main pipeline >> %LOG_FILE%

REM Run the main pipeline
python src\run_daily.py >> %LOG_FILE% 2>&1
set PIPELINE_EXIT_CODE=%errorlevel%

if %PIPELINE_EXIT_CODE% equ 0 (
    echo.
    echo ====================================================================
    echo   Pipeline Execution Completed Successfully!
    echo ====================================================================
    echo.
    echo Output locations:
    echo   - Reports: .\reports\
    echo   - Data: .\data\
    echo   - Forms: .\forms\
    echo   - Logs: %LOG_FILE%
    echo.
    echo [%DATE% %TIME%] Pipeline completed successfully >> %LOG_FILE%
) else (
    echo.
    echo ====================================================================
    echo   Pipeline Execution Completed with Warnings/Errors
    echo ====================================================================
    echo.
    echo Please check %LOG_FILE% for details.
    echo.
    echo [%DATE% %TIME%] Pipeline completed with exit code %PIPELINE_EXIT_CODE% >> %LOG_FILE%
)

REM Show summary of outputs
echo.
echo [INFO] Summary of generated files:
if exist "reports\latest\probabilities.csv" (
    echo   [FOUND] reports\latest\probabilities.csv
) else (
    echo   [MISSING] reports\latest\probabilities.csv
)
if exist "reports\latest\summary.md" (
    echo   [FOUND] reports\latest\summary.md
) else (
    echo   [MISSING] reports\latest\summary.md
)

echo.
echo ====================================================================
echo   Execution Complete - Check %LOG_FILE% for Full Details
echo ====================================================================
echo.

REM Display troubleshooting tips if there were errors
if %INSTALL_EXIT_CODE% neq 0 (
    echo.
    echo ====================================================================
    echo   TROUBLESHOOTING TIPS - Dependency Installation Issues
    echo ====================================================================
    echo.
    echo Common solutions:
    echo   1. Check your internet connection
    echo   2. Try running as Administrator (right-click, Run as administrator^)
    echo   3. Manually install problematic packages:
    echo      - Open Command Prompt
    echo      - Navigate to this folder
    echo      - Run: venv\Scripts\activate
    echo      - Run: pip install --upgrade pip
    echo      - Run: pip install -r requirements.txt -v
    echo.
    echo   4. Check if antivirus is blocking pip
    echo   5. Try using a different network or VPN
    echo   6. Review %LOG_FILE% for specific error messages
    echo.
    echo For Python 3.14 compatibility issues:
    echo   - Some packages may not have wheels for Python 3.14 yet
    echo   - Consider using Python 3.11 or 3.12 instead
    echo   - Download from: https://www.python.org/downloads/
    echo.
    echo ====================================================================
    echo.
)

echo Press any key to exit...
pause >nul

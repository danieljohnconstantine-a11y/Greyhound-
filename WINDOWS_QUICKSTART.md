# Windows Quick Start Guide

## One-Click Execution

This repository includes `run_greyhound_local.bat` for easy Windows execution.

### Prerequisites

- **Windows OS** (Windows 7 or later)
- **Python 3.8 - 3.12** installed and added to PATH
  - **Recommended:** Python 3.11 or 3.12 for best compatibility
  - Download from: https://www.python.org/downloads/
  - **Important:** Check "Add Python to PATH" during installation
  - **Note:** Python 3.14+ may have compatibility issues with some packages (pandas, lxml)
  - If you have Python 3.14, consider installing Python 3.12 alongside it

### Usage

1. **Download** the repository as a ZIP file or clone it
2. **Extract** the ZIP to a folder (e.g., `C:\Greyhound`)
3. **Double-click** `run_greyhound_local.bat`

That's it! The script will:
- ✓ Create a Python virtual environment (first run only)
- ✓ Install all required dependencies
- ✓ Run validation tests
- ✓ Execute the main greyhound analytics pipeline
- ✓ Generate reports in the `reports/` folder
- ✓ Save execution logs to `run_log.txt`

### What the Script Does

1. **Environment Setup**
   - Creates `venv/` folder for isolated Python environment
   - Installs dependencies from `requirements.txt`
   - Creates necessary directories (`outputs/`, `data/`, `forms/`, `reports/`)

2. **Validation**
   - Runs `validate_speed_implementation.py` to verify the implementation
   - Tests Section 2 parsing and per-dog Speed_kmh calculation

3. **Main Pipeline**
   - Executes `src/run_daily.py`
   - Fetches form data
   - Processes greyhound race information
   - Generates probability reports

### Output Locations

After execution, check these folders:

- **`reports/latest/`** - Latest race predictions and probabilities
  - `probabilities.csv` - Probability data
  - `summary.md` - Quick summary of top picks
- **`data/`** - Parsed data files
- **`forms/`** - Downloaded form PDFs
- **`run_log.txt`** - Detailed execution log

### Troubleshooting

**"Python is not installed or not in PATH"**
- Install Python from https://www.python.org/downloads/
- During installation, check "Add Python to PATH"
- Restart your computer after installation

**"Failed to create virtual environment"**
- Delete the `venv` folder if it exists
- Run the batch script again

**"Failed to install dependencies"** or **Dependency installation issues**
- **Check internet connection** - pip needs to download packages
- **Try running as Administrator** - Right-click `run_greyhound_local.bat` → Run as administrator
- **Python 3.14 compatibility** - Some packages may not have wheels for Python 3.14 yet
  - **Recommended:** Use Python 3.11 or 3.12 for best compatibility
  - Download from: https://www.python.org/downloads/
  - Python 3.8, 3.9, 3.10, 3.11, or 3.12 are all supported
- **Check antivirus** - Some antivirus software blocks pip installations
- **Network issues** - Try a different network or disable VPN temporarily
- **Manual installation:**
  ```cmd
  cd path\to\Greyhound
  venv\Scripts\activate
  pip install --upgrade pip
  pip install -r requirements.txt -v
  ```
- Check `run_log.txt` for specific error messages

**Package-specific issues:**
- **lxml** - May require Microsoft C++ Build Tools on some systems
  - Download: https://visualstudio.microsoft.com/visual-cpp-build-tools/
- **pandas** - Usually installs fine, but needs numpy which may have compatibility issues with Python 3.14
  - Solution: Use Python 3.11 or 3.12

**Script runs but no outputs**
- Check `run_log.txt` for error messages
- Ensure you have form PDFs in the `forms/` folder
- The pipeline may not generate outputs if there's no race data available

### Manual Execution (Alternative)

If the batch script doesn't work, you can run manually:

```cmd
cd path\to\Greyhound
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python validate_speed_implementation.py
python src\run_daily.py
```

### Support

For issues or questions:
- Check `run_log.txt` for detailed error messages
- Review `VALIDATION_REPORT.md` for implementation details
- See `QUICK_START.md` for integration guide

## Advanced Usage

### Running Specific Components

After the initial setup (venv created and dependencies installed), you can run specific components:

```cmd
# Activate environment first
venv\Scripts\activate

# Run validation only
python validate_speed_implementation.py

# Run main pipeline
python src\run_daily.py

# Run example pipeline
python examples\example_pipeline.py

# Run demonstration
python examples\demonstrate_fix.py
```

### Customizing the Script

Edit `run_greyhound_local.bat` to:
- Change output locations
- Modify which scripts run
- Add custom processing steps
- Adjust logging verbosity

### Scheduling Automatic Runs

Use Windows Task Scheduler to run `run_greyhound_local.bat` automatically:

1. Open **Task Scheduler**
2. Create **New Task**
3. Set **Trigger** (e.g., daily at specific time)
4. Set **Action** to run `run_greyhound_local.bat`
5. Configure to run whether user is logged on or not

---

**Status:** Ready for local Windows execution ✓

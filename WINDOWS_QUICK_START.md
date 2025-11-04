# Windows Quick Start Guide

This guide explains how to run the Greyhound Race Form Parser on Windows with a single click.

## Prerequisites

1. **Python 3.8 or higher** installed on your system
   - Download from: https://www.python.org/downloads/
   - During installation, make sure to check "Add Python to PATH"

2. **PDF files** containing greyhound race forms

## Quick Start (Single Click)

### Option 1: Use the Batch File (Recommended)

1. **Download the repository** as a ZIP file from GitHub
2. **Extract** the ZIP file to a folder on your computer
3. **Place your PDF files** in the `/data` folder
4. **Double-click** `run_parser.bat`
5. **Wait** for the parser to complete
6. **Find your results** in the `/outputs` folder

The batch file will:
- ✅ Check if Python is installed
- ✅ Install required dependencies automatically
- ✅ Create necessary folders
- ✅ Run the enhanced parser with validation
- ✅ Show you where the output files are saved

### Option 2: Manual Command Line

If you prefer using the command line:

1. Open Command Prompt (cmd.exe)
2. Navigate to the repository folder:
   ```
   cd C:\path\to\Greyhound-
   ```
3. Install dependencies (first time only):
   ```
   pip install -r requirements.txt
   ```
4. Run the parser:
   ```
   python main_enhanced.py
   ```

## Output Files

After running the parser, you'll find these files in `/outputs`:

- **todays_form_YYYYMMDD_HHMMSS.csv** - CSV format with all 62 fields
- **todays_form_YYYYMMDD_HHMMSS.xlsx** - Excel format with all 62 fields
- **parse_enhanced.log** - Detailed log file with validation results

## Troubleshooting

### "Python is not installed or not in PATH"

- Install Python from https://www.python.org/downloads/
- During installation, check the box "Add Python to PATH"
- Restart your computer after installation

### "Failed to install dependencies"

Open Command Prompt and run:
```
pip install pandas>=2.2.2 pdfplumber>=0.10.0 openpyxl>=3.1.0
```

### "No PDF files found"

- Make sure you placed PDF files in the `/data` folder
- Check that the files have a `.pdf` extension
- The parser looks for PDF files in `/data` by default

### Parser runs but no output

- Check the console output for error messages
- Review the `parse_enhanced.log` file in `/outputs`
- Make sure your PDF files are valid greyhound race forms

## What Gets Extracted?

The parser extracts **62 fields** from each greyhound in the race forms:

**Basic Info:**
- Track, Race, Box, DogName, Trainer, Grade, Distance, RaceDate

**Performance:**
- Form, WinRate, PlaceRate, Odds, BestTime, Margin, Sectionals

**Career Stats:**
- Starts, Wins, Seconds, Thirds, CareerPrizeMoney, CareerBest

**Details:**
- Owner, Sire, Dam, Age, Sex, Color, Weight
- TrainerWinRate, TrainerCity, TrainerState
- Performance metrics, career records, track conditions

And many more!

## Daily Workflow

1. Download new race form PDFs from your source
2. Place them in the `/data` folder
3. Double-click `run_parser.bat`
4. Wait for processing to complete
5. Open the Excel/CSV files in `/outputs`
6. Use the data for your analysis

## Need More Help?

See the other documentation files:
- `INSTALLATION.md` - Detailed installation guide
- `TODAYS_FORM_README.md` - Output file format details
- `README.md` - General project information

## Support

If you encounter issues, check the log file in `/outputs/parse_enhanced.log` for detailed error messages and validation results.

# Today's Form - Master Daily Greyhound Analysis

## Overview

The `main_enhanced.py` script generates a unified daily master file called **"todays_form"** that combines all greyhound race data from multiple PDFs into a single comprehensive Excel and CSV report.

## Output Files

When you run `python main_enhanced.py`, the following files are generated in the `/outputs` directory:

- **`todays_form_<timestamp>.xlsx`** - Excel format with all 62 fields
- **`todays_form_<timestamp>.csv`** - CSV format with identical data
- **`parse_enhanced.log`** - Detailed extraction log with validation results

### Filename Format

```
todays_form_YYYYMMDD_HHMMSS.xlsx
todays_form_YYYYMMDD_HHMMSS.csv
```

Example: `todays_form_20251103_010100.xlsx`

## Data Structure

### Column Order

The first 4 columns are always:
1. **Track** - Track code (e.g., MAND, CAPA, DRWN)
2. **Race** - Race number
3. **Box** - Box number (1-8)
4. **DogName** - Name of the greyhound

Followed by 58 additional fields including:
- Trainer, Grade, Distance, RaceDate
- Form, WinRate, PlaceRate, Odds
- BestTime, Margin, Sectionals (1-3)
- Starts, Wins, Seconds, Thirds
- CareerPrizeMoney, Age, Sex, Color, Weight
- Owner, Sire, Dam
- Performance metrics and track records
- Comments, Notes, SourcePDF

**Total: 62 fields per dog**

### Row Ordering

All records are sorted by:
1. **Track** (alphabetically)
2. **Race** (numerically ascending)
3. **Box** (numerically ascending)

Example order:
```
CAPA Race 7 Box 1
CAPA Race 7 Box 2
CAPA Race 7 Box 3
MAND Race 7 Box 1
MAND Race 7 Box 2
```

## Validation

The script performs comprehensive validation to ensure **PDF=Excel consistency**:

### 1. Required Columns Check
- Verifies that Track, Race, Box, and DogName columns are present

### 2. Speed Fields Verification
- Validates all speed-related fields:
  - BestTime, Sectional1, Sectional2, Sectional3
  - SplitAvg, EarlySpeed, ClosingSpeed
- Reports population percentage for each field

### 3. Data Dimensions
- Confirms row count (total dogs)
- Confirms column count (62 fields)

### 4. Race/Box Ordering
- Verifies proper ascending order by Race then Box

### 5. Validation Summary

At completion, the script displays:
```
Tracks: X | Races: Y | Dogs: Z | Speed fields verified OK | PDF=Excel verified.
```

This confirms:
- Total number of tracks processed
- Total number of races extracted
- Total number of dog records
- Speed-related fields are consistent
- PDF data matches Excel output

## Usage

### Standard Workflow

1. **Add PDFs to `/data` directory**
   ```bash
   # Place all PDF files in /data or subdirectories
   cp *.pdf /data/
   ```

2. **Run the parser**
   ```bash
   python main_enhanced.py
   ```

3. **Collect outputs from `/outputs`**
   ```bash
   # Files will be in /outputs directory
   ls -lh outputs/todays_form_*
   ```

### Example Output

```
================================================================================
🐕 GREYHOUND FORM EXTRACTION PIPELINE - TODAY'S FORM
================================================================================

Processing PDFs from: data

Validating data integrity...

Generating reports...
✅ CSV saved: outputs/todays_form_20251103_010100.csv
✅ Excel saved: outputs/todays_form_20251103_010100.xlsx

================================================================================
✅ EXTRACTION COMPLETE - TODAY'S FORM
================================================================================

📊 Summary:
  Tracks: 2 | Races: 2 | Dogs: 20 | Speed fields verified OK | PDF=Excel verified.

📁 Output Files:
  • CSV report: outputs/todays_form_20251103_010100.csv
  • Excel report: outputs/todays_form_20251103_010100.xlsx
  • Log file: outputs/parse_enhanced.log
```

## Log File Details

The `parse_enhanced.log` file contains:

1. **Extraction Statistics**
   - Total records, fields, tracks, races
   - Per-track breakdown (races and dogs per track)
   - Field coverage analysis (% populated for each field)
   - Race/Box ordering verification

2. **Data Validation**
   - Required columns check
   - Speed fields population rates
   - Data dimensions verification
   - Race/Box ordering confirmation
   - PDF=Excel verification status

3. **Final Summary**
   - Validation summary line
   - File paths for outputs

## File Management

### Overwriting Policy

Each run creates new timestamped files. The script does **not** automatically:
- Delete old files
- Archive previous versions
- Overwrite existing files

To manage old files:

```bash
# Keep only the most recent file
cd outputs
ls -t todays_form_*.xlsx | tail -n +2 | xargs rm

# Archive old files
mkdir archive
mv todays_form_2025*.xlsx archive/
```

### Recommended Practice

For daily operations:
1. Run parser once per day with all PDFs
2. Review validation summary
3. Check log file for any warnings
4. Archive or delete previous day's files
5. Use the latest `todays_form_<timestamp>` files for analysis

## Troubleshooting

### No Data Extracted

```
⚠️  No data extracted from PDFs.
Please add PDF files to /path/to/data and try again.
```

**Solution**: Add PDF files to the `/data` directory

### Missing Dependencies

```
ModuleNotFoundError: No module named 'pandas'
```

**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Validation Warnings

Check `parse_enhanced.log` for detailed information about:
- Missing columns
- Low population rates for key fields
- Race/Box ordering issues
- Field extraction problems

## Integration with Analysis Tools

The todays_form files can be imported into:

- **Excel**: Direct open `.xlsx` file
- **Python/Pandas**: 
  ```python
  import pandas as pd
  df = pd.read_csv('outputs/todays_form_20251103_010100.csv')
  ```
- **R**: 
  ```r
  df <- read.csv('outputs/todays_form_20251103_010100.csv')
  ```
- **SQL Databases**: Import CSV for querying
- **BI Tools**: Tableau, Power BI, etc.

## Benefits

1. **Single Source of Truth**: One file per day with all tracks/races
2. **Validated Data**: Automatic PDF=Excel consistency checks
3. **Complete Fields**: All 62 fields extracted and organized
4. **Proper Ordering**: Races and boxes in logical sequence
5. **Audit Trail**: Detailed log file for verification
6. **Multi-Format**: Both CSV and Excel for compatibility
7. **Timestamped**: Clear versioning with timestamp in filename

## See Also

- `INSTALLATION.md` - Setup instructions
- `PIPELINE_README.md` - Original pipeline documentation
- `IDENTIFIED_FIELDS.md` - Complete field descriptions
- `column_template.csv` - Field reference

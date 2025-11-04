# Greyhound Form Extraction Pipeline

## Overview

This is a fully automated, layout-aware parser that extracts data from greyhound race form PDFs. It processes multi-column racing forms from tracks like BRHG, CAPAG, DRWNG, etc., and combines all data into unified CSV and Excel reports.

## Features

- **Layout-aware parsing** using pdfplumber
- **Multi-column detection** for complex race forms
- **Automatic race detection** via regex patterns
- **Data extraction** for all visible fields:
  - Track code
  - Race number
  - Distance
  - Box number (1-10)
  - Dog name
  - Trainer
  - Prize money
  - Margins (career record)
  - And more
- **Combined output** merging all tracks
- **Dual format reports**: CSV and Excel with timestamps
- **Detailed logging** with extraction metrics

## Requirements

```bash
pip install pdfplumber pandas openpyxl xlsxwriter PyMuPDF
```

All requirements are also in `requirements.txt`.

## Directory Structure

```
.
├── data/              # Place your PDF files here (subdirectories supported)
├── outputs/           # Generated reports appear here (auto-created)
├── src/
│   └── parser_step1_complete_layoutaware.py  # Core parser module
└── main.py            # Main controller script
```

## Usage

### Basic Usage

1. Place your PDF files in the `/data` directory (or any subdirectory within it)
2. Run the main script:

```bash
python main.py
```

3. Check the `/outputs` directory for results:
   - `greyhound_analysis_<timestamp>.csv` - CSV format
   - `greyhound_analysis_<timestamp>.xlsx` - Excel format
   - `parse.log` - Detailed extraction log

### Testing Individual PDFs

You can test the parser on a single PDF:

```bash
python src/parser_step1_complete_layoutaware.py <path_to_pdf>
```

Example:
```bash
python src/parser_step1_complete_layoutaware.py data/QSTR_2025-09-08.pdf
```

## Output Format

Both CSV and Excel files contain the following columns:

| Column | Description |
|--------|-------------|
| Track | Track code (e.g., QSTR, CAPA, DRWN) |
| RaceNo | Race number |
| Distance | Race distance in meters |
| Box | Box/starting position (1-10) |
| DogName | Name of the greyhound |
| Trainer | Trainer name |
| PrizeMoney | Career prize money |
| Odds | Odds (if available) |
| Margins | Career record (format: W-P-S) |
| Comment | Additional comments |
| RaceTime | Race time (if available) |

## Logging

The pipeline generates detailed logs in `outputs/parse.log` including:

- Files processed
- Pages per file
- Dogs extracted per race
- Per-track statistics
- Data quality metrics
- Missing data flags

## Extraction Statistics

After processing, the log file includes:

- Total records extracted
- Number of unique tracks
- Number of unique races
- Dogs per race (min/max/avg)
- Per-track breakdown
- Data quality metrics

## Supported PDF Formats

The parser is designed for Racing & Sports multi-column PDFs with:

- Race headers like "Race No 08"
- Box-numbered entries (1-10)
- Form guide numbers (e.g., "13582")
- Dog names in title case
- Trainer names
- Prize money in format "$X,XXX"
- Career records in format "N-N-N"

## Troubleshooting

### No data extracted

- Check that PDFs are in the correct format
- Verify PDFs contain race form data (not results)
- Check `parse.log` for specific errors

### Missing fields

- Some fields may not be present in all PDFs
- Check the log for data quality metrics
- Missing trainers or other fields are logged

### Duplicate entries

- The parser automatically deduplicates based on Track, RaceNo, and Box
- If you see duplicates, check if they're from different races

## Examples

### Running with subdirectories

```bash
# PDFs can be in any subdirectory
data/
  ├── QSTR_2025-09-08.pdf
  ├── rns/
  │   └── 2025-10-07/
  │       ├── MANDG0710form.pdf
  │       └── QLAKG0710form.pdf
  └── archive/
      └── old_forms.pdf

python main.py  # Will find all PDFs recursively
```

### Output example

```
============================================================
🐕 GREYHOUND FORM EXTRACTION PIPELINE - STEP 1
============================================================

Found 5 PDF files to process

[1/5] 📄 Processing: QSTR_2025-09-08.pdf
    ✓ Extracted 10 records

...

============================================================
✅ ALL TRACKS COMBINED — STEP 1 COMPLETE
============================================================

📊 Summary:
  • Total records: 48
  • Unique tracks: 5
  • Unique races: 5
  • CSV report: outputs/greyhound_analysis_20251102_044851.csv
  • Excel report: outputs/greyhound_analysis_20251102_044851.xlsx
  • Log file: outputs/parse.log
```

## Known Limitations

- Currently extracts dog entries only (not full race results)
- Some fields may be empty if not present in the source PDF
- Odds extraction is limited (most PDFs don't include odds)
- Race times are not typically present in form guides

## Future Enhancements

- Add support for race results PDFs
- Extract historical performance data
- Add odds extraction when available
- Support for additional track codes
- PDF validation before processing

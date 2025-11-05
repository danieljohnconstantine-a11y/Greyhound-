# Enhanced PDF Parser - Extracted Fields Documentation

This document lists all fields extracted by the enhanced PDF parser (`src/parse_pdf_enhanced.py`) from greyhound racing form PDFs.

## Field Categories

### 1. Basic Information
- **name**: Dog's name (e.g., "ZOMBIE BOSS")
- **box**: Starting box number (1-10)
- **weight**: Weight in kg (e.g., "0", "32.5")
- **color**: Color description (e.g., "blu", "red/fwn", "bl")
- **age**: Age in years (e.g., "2", "3")
- **sex**: Sex (D=Dog/Male, B=Bitch/Female, W=?, H=?)

### 2. Pedigree
- **sire**: Father's name (e.g., "AUSSIE INFRARED")
- **dam**: Mother's name (e.g., "UNDERCOVER BOSS")

### 3. Trainer & Owner
- **trainer**: Trainer's name (e.g., "ADAM POULTER")
- **owner**: Owner's name or syndicate (e.g., "Adam Poulter" or "Enigma Racing Synd S Bolton,P Caldow...")

### 4. Distance Information
- **raced_distance**: Distance range raced (e.g., "312-383")
- **winning_distance**: Distance and number of wins (e.g., "312m (4)" or "NA")

### 5. Career Statistics
- **horse_record**: Career record as wins-places-starts (e.g., "4-15-46")
- **horse_win_pct**: Win percentage (e.g., "9")
- **horse_place_pct**: Place percentage (e.g., "42")

### 6. Jockey/Trainer Statistics
- **jockey_50s**: Jockey stats at 50m splits (usually "-" for greyhounds)
- **jockey_350s**: Jockey stats at 350m splits (usually "-" for greyhounds)
- **trainer_50s**: Trainer record at 50m (e.g., "4-20-50 8%-48%")
- **trainer_50s_pct**: Trainer win-place percentage at 50m (e.g., "8%-48%")
- **trainer_350s**: Trainer record at 350m (e.g., "57-120-350 16%-50%")
- **trainer_350s_pct**: Trainer win-place percentage at 350m

### 7. Performance Metrics
- **car_pm_per_start**: Career prize money per start (e.g., "$143")
- **twelve_month_pm_per_start**: Last 12 months prize money per start (e.g., "$143")
- **api**: API (Average Performance Index) rating (e.g., "0.1", "2.9")
- **rtc_per_km**: Run Time Class per kilometer (e.g., "47")
- **rdist_tc**: Recent Distance Time Class (e.g., "15.133")
- **dls**: Days Last Start - days since last race (e.g., "14")
- **dlw**: Days Last Win - days since last win (e.g., "80")
- **dod**: Days Off Distance - performance adjustment (e.g., "-5.8", "1")

### 8. Grade Statistics
Records at different grade levels (format: wins-places-starts win%-place%):
- **grade_g1**: Grade 1 record (usually "-" if not applicable)
- **grade_g2**: Grade 2 record
- **grade_g3**: Grade 3 record
- **grade_lr**: Long Range record
- **grade_fu**: First Up record (e.g., "0-1-1 0%-100%")
- **grade_2u**: Second Up record (e.g., "1-0-1 100%-")
- **grade_3u**: Third Up record (e.g., "0-0-1 0%-0%")

### 9. Track Condition Statistics
Records on different track surfaces (format: wins-places-starts win%-place%):
- **condition_firm**: Firm track record
- **condition_good**: Good track record
- **condition_soft**: Soft track record
- **condition_heavy**: Heavy track record
- **condition_aw**: All Weather track record (e.g., "4-15-46 9%-42%")
- **condition_turf**: Turf track record

### 10. Performance Records
Various performance records (format: wins-places-starts win%-place%):
- **car_record**: Career record (e.g., "4-15-46 9%-42%")
- **twelve_month_record**: Last 12 months record (e.g., "4-15-46 9%-42%")
- **course_record**: Record at this course (e.g., "4-15-46 9%-42%")
- **distance_record**: Record at this distance (e.g., "4-12-35 11%-45%")
- **clock_w_record**: Clock wins record
- **aclock_w_record**: Adjusted clock wins record

### 11. Race History
- **race_history**: List of past race entries. Each entry is a string containing:
  - Finishing position (e.g., "3rd of 5")
  - Date (e.g., "20/08/2025")
  - Venue (e.g., "DARWIN")
  - Margin (e.g., "8.3 Lengths")
  - Distance (e.g., "312m")
  - State of Track (SOT)
  - Race Start Type (RST)
  - Grade (GR)
  - Race name
  - Prize money
  - API for that race
  - Race time
  - Sectional times
  - Box position (BP)
  - Odds
  - Prize won (if applicable)
  - Trainer
  - Ongoing winners
  - Track direction
  - Winner, second, third place finishers
  - Settled positions (if applicable)

Example race history entry:
```
3rd of 5 20/08/2025 DARWIN Margin 8.3 Lengths Distance 312m SOT G RST GR 5 /MDN Race ASIAN-UNITED FOOD SERVICE DASH DIVISION1 Prize $1,200 API 0.09 Race Time 0:18.42 Sec Time 7.32 BP 1 Odds 0.4F Prize Won $180 Trainer Adam Poulter Ongoing Winners 00-01-05 Track Direction Anti-Clockwise Winner It's A Gem (4) Second Mojo Max (2)
```

### 12. Metadata
- **track**: Track code (e.g., "DRWN" for Darwin, "CANN" for Cannington)
- **date**: Date of the form guide (e.g., "2025-09-03")
- **race**: Race number (e.g., 1, 2, 3)

## Output Formats

The parser can output data in two formats:

### CSV Format
Standard CSV with all fields as columns. Race history is stored as a JSON-encoded list within the CSV cell.

### JSON Format
Full JSON with proper data types. Race history is stored as a list of strings.

## Usage Examples

### Command Line
```bash
# Parse all PDFs in forms directory
python src/parse_pdf_enhanced.py --forms forms --out data/parsed.csv --json data/parsed.json

# Parse only first PDF (for testing)
python src/parse_pdf_enhanced.py --forms forms --sample
```

### Python API
```python
from src.parse_pdf_enhanced import parse_pdf_enhanced

# Parse a single PDF
dogs = parse_pdf_enhanced('forms/DRWN_2025-09-03.pdf')

# Access dog information
for dog in dogs:
    print(f"{dog['name']} - Box {dog['box']}")
    print(f"  Career: {dog['horse_record']} ({dog['horse_win_pct']}%-{dog['horse_place_pct']}%)")
    print(f"  Race history: {len(dog['race_history'])} entries")
```

## Data Quality Notes

- Some fields may be `None` or empty string if not present in the PDF
- Race history parsing captures full text but may need additional parsing for structured data
- Percentage fields are stored as strings (e.g., "9" for 9%)
- Money fields include currency symbol (e.g., "$143")
- Record fields combine multiple values with spaces (e.g., "4-15-46 9%-42%")

## Fields That May Need Further Parsing

If you need structured data from these fields, additional parsing may be required:

1. **race_history**: Currently stored as full text strings. Could be parsed into structured dict with individual fields.
2. **owner**: May contain multiple names and addresses that could be split.
3. **trainer_50s** and **trainer_350s**: Contain both record and percentage that could be separated.
4. **grade_fu**, **grade_2u**, **grade_3u**: Contain both record and percentage.
5. All **condition_*** fields: Contain both record and percentage.
6. All **_record** fields: Contain both record and percentage.

## Comparison with Problem Statement

Based on the problem statement examples, this parser extracts all the information shown:

✅ Basic info (name, box, weight, color, age, sex)
✅ Pedigree (sire and dam)
✅ Trainer and owner
✅ Distance information
✅ Career statistics
✅ Jockey/Trainer statistics (j50s, j350s, t50s, t350s)
✅ Performance metrics (CarPM/s, 12mPM/s, API, RTC/km, RDistTC, DLS, DLW, DOD)
✅ Grade statistics (G1, G2, G3, LR, FU, 2U, 3U)
✅ Track condition statistics (Firm, Good, Soft, Heavy, AW, Turf)
✅ Performance records (Car, 12m, Crs, Dist, ClockW, AClockW)
✅ Complete race history with all details

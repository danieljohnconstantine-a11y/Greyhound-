# Speed_kmh Calculation Fix

## Overview

This fix addresses the critical issue where all dogs in a race had identical `Speed_kmh` values. The problem was that the calculation used race-level data instead of per-dog Section 2 (recent runs) data.

## Solution

The fix implements three new modules:

### 1. `src/parser.py` - Section 2 Parsing

Parses individual dog's recent run data from Section 2 text.

**Key Functions:**
- `_extract_recent_runs(section2_text)` - Extracts all distance/time pairs
- `_normalize_section2(runs)` - Builds S2_AllSpeeds list and identifies fastest run
- `parse_dog_form(form_data)` - Complete parsing for a single dog
- `parse_race_card(race_card)` - Batch parsing for all dogs in a race

**Supported Formats:**
- `400m 23.91` (space separator)
- `520m:30.12` (colon separator)
- `520m–29.94s` (en-dash with optional 's')
- `520m—29.94` (em-dash)

**Validation:**
- Distance: 200-800 meters
- Time: 15-60 seconds
- Speed calculated as: (distance / time) × 3.6 km/h

### 2. `src/features_advanced.py` - Speed Calculation

Calculates `Speed_kmh` from parsed Section 2 data.

**Key Functions:**
- `calculate_speed_kmh(dog_data)` - Returns max(S2_AllSpeeds) or None
- `build_advanced_features(dog_data)` - Adds Speed_kmh to dog data
- `process_race_features(race_card)` - Batch feature building

**Important:**
- Uses ONLY Section 2 data
- No fallback to race-level RaceTime or CurrentDistance
- Returns `None` if Section 2 data unavailable
- Filters invalid values (NaN, 0, negative)

### 3. `src/exporter.py` - Validation & Export

Validates that Speed_kmh varies per race and exports data.

**Key Functions:**
- `validate_speed_uniqueness(df)` - Validates Speed_kmh uniqueness per race
- `export_to_excel(df, path)` - Export with validation
- `export_to_csv(df, path)` - Export with validation

**Validation Logs:**
- `[S2][OK]` - Race has unique speeds (good!)
- `[S2][DUP]` - Race has duplicate speeds (bad!)
- `[S2][MISS]` - Race has all missing speeds

## Usage

### Basic Usage

```python
from parser import parse_race_card
from features_advanced import process_race_features
from exporter import export_to_csv

# 1. Parse Section 2 data
parsed_data = parse_race_card(raw_race_data)

# 2. Build Speed_kmh feature
featured_data = process_race_features(parsed_data)

# 3. Export with validation
df = pd.DataFrame(featured_data)
export_to_csv(df, 'output.csv', validate=True)
```

### Example Output

```
[S2][OK] SALE R1: 3 unique speeds among 3 dogs
[S2][OK] RICH R2: 3 unique speeds among 3 dogs

Summary:
  Total races: 2
  Races with unique speeds: 2
  Races with duplicate speeds: 0

✓ SUCCESS! All races have unique Speed_kmh values!
```

## Running the Example

```bash
python3 examples/example_pipeline.py
```

This demonstrates the complete pipeline with sample data.

## Running Tests

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

All 27 tests should pass.

## Integration

To integrate with existing pipeline:

1. After parsing form PDFs, pass data through `parse_race_card()`
2. Before feature engineering, run `process_race_features()`
3. Before final export, validate with `validate_speed_uniqueness()`

## Success Criteria

- [x] Each race shows varied per-dog speeds
- [x] No duplicate Speed_kmh per race
- [x] Section 2 parsing supports multiple formats
- [x] Comprehensive test coverage
- [x] Validation logging for monitoring

## Files Added

- `src/parser.py` - Section 2 parsing (3.9 KB)
- `src/features_advanced.py` - Speed calculation (2.0 KB)
- `src/exporter.py` - Validation & export (5.7 KB)
- `tests/test_parser.py` - Parser tests (6.0 KB)
- `tests/test_features_advanced.py` - Features tests (3.1 KB)
- `tests/test_exporter.py` - Exporter tests (3.4 KB)
- `examples/example_pipeline.py` - Demo (4.7 KB)

## Dependencies Added

- `openpyxl==3.1.5` (for Excel export)

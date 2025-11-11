# Quick Start - Integrating Speed_kmh Fix

## Problem Statement
All dogs in each race had identical `Speed_kmh` values because the calculation used race-level data instead of per-dog Section 2 (recent runs) data.

## Solution
Three new modules parse Section 2 data and calculate unique Speed_kmh for each dog.

## 5-Minute Integration

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Import Modules
```python
from parser import parse_race_card
from features_advanced import process_race_features
from exporter import validate_speed_uniqueness
```

### 3. Add to Your Pipeline

**Before (example):**
```python
# Old code that produced identical speeds
df = pd.DataFrame(raw_race_data)
# ... feature engineering ...
df.to_csv('output.csv')
```

**After:**
```python
# Parse Section 2 data
parsed_data = parse_race_card(raw_race_data)

# Build Speed_kmh feature
featured_data = process_race_features(parsed_data)

# Convert to DataFrame
df = pd.DataFrame(featured_data)

# Validate before export
validate_speed_uniqueness(df, verbose=True)

# Export
df.to_csv('output.csv', index=False)
```

### 4. Data Format Requirements

Your `raw_race_data` should include a `section2` field for each dog:

```python
raw_race_data = [
    {
        'Track': 'SALE',
        'RaceNumber': 1,
        'Box': 1,
        'Runner': 'FAST PUP',
        'section2': '400m 23.50, 520m:29.80, 450m–25.20s'  # Recent runs
    },
    # ... more dogs
]
```

**Supported Section 2 Formats:**
- `400m 23.91` (space separator)
- `520m:30.12` (colon separator)
- `520m–29.94s` (en-dash with optional 's')
- `520m—29.94` (em-dash)

### 5. Verify It's Working

Run validation to check for duplicate speeds:

```python
from exporter import validate_speed_uniqueness
stats = validate_speed_uniqueness(df, verbose=True)

print(f"Races with unique speeds: {stats['races_with_unique_speeds']}")
print(f"Races with duplicates: {stats['races_with_duplicates']}")
```

**Expected output:**
```
[S2][OK] SALE R1: 3 unique speeds among 3 dogs
[S2][OK] RICH R2: 4 unique speeds among 4 dogs

Races with unique speeds: 2
Races with duplicates: 0
```

## Testing

Run the example to verify everything works:

```bash
python3 examples/example_pipeline.py
```

You should see output like:
```
[S2][OK] RICH R2: 3 unique speeds among 3 dogs
[S2][OK] SALE R1: 3 unique speeds among 3 dogs

✓ SUCCESS! All races have unique per-dog speeds.
```

## Common Issues

### Issue: "All speeds are None"
**Cause:** Section 2 data is missing or in wrong format  
**Fix:** Check that your data includes `section2` field with valid distance/time pairs

### Issue: "Still getting duplicate speeds"
**Cause:** Section 2 data is the same for all dogs (rare)  
**Fix:** Verify your data source provides per-dog Section 2 information

### Issue: "Import errors"
**Cause:** Missing dependencies  
**Fix:** Run `pip install -r requirements.txt`

## Next Steps

1. ✓ Integrate into your pipeline
2. ✓ Run validation on first batch
3. Monitor Section 2 coverage:
   ```python
   covered = df['Speed_kmh'].notna().sum()
   total = len(df)
   print(f"Section 2 coverage: {covered/total*100:.1f}%")
   ```
4. Review logs for any `[S2][DUP]` warnings
5. Adjust distance/time validation ranges if needed (in `parser.py`)

## Full Documentation

- `SPEED_FIX_README.md` - Complete usage guide
- `IMPLEMENTATION_SUMMARY.md` - Technical details
- `examples/example_pipeline.py` - Working example
- `examples/demonstrate_fix.py` - Before/after comparison

## Support

All 27 unit tests pass. If you encounter issues:
1. Run tests: `python3 -m unittest discover -s tests`
2. Check examples: `python3 examples/example_pipeline.py`
3. Review logs for `[S2][...]` messages

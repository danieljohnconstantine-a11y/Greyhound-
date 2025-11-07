# Enhanced PDF Parser Usage Guide

## Overview

The enhanced PDF parser (`src/parse_pdf_enhanced.py`) extracts comprehensive greyhound racing information from Racing & Sports PDF form guides. It captures detailed information for each dog including:

- 52 structured data fields per dog
- Complete race history with all details
- Performance metrics and statistics
- Pedigree and connections information

## Quick Start

### Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```bash
# Parse all PDFs in the forms directory
python src/parse_pdf_enhanced.py --forms forms --out data/parsed.csv --json data/parsed.json

# Parse only one PDF for testing
python src/parse_pdf_enhanced.py --forms forms --sample
```

### See What's Extracted

```bash
# Run the demonstration script
python demo_extraction.py
```

This will show all fields extracted from sample dogs, formatted for easy reading.

## Python API

### Parse a Single PDF

```python
from src.parse_pdf_enhanced import parse_pdf_enhanced

# Parse a PDF file
dogs = parse_pdf_enhanced('forms/DRWN_2025-09-03.pdf')

# Access dog information
for dog in dogs:
    print(f"{dog['name']} - Box {dog['box']} - Race {dog['race']}")
    print(f"  Trainer: {dog['trainer']}")
    print(f"  Career: {dog['horse_record']} ({dog['horse_win_pct']}%-{dog['horse_place_pct']}%)")
    print(f"  API: {dog['api']}")
    print(f"  Race history: {len(dog['race_history'])} entries")
```

### Parse Multiple PDFs

```python
from src.parse_pdf_enhanced import parse_folder_enhanced
import pandas as pd

# Parse entire directory
df = parse_folder_enhanced('forms')

# Filter and analyze
print(f"Total dogs: {len(df)}")
print(f"Tracks: {df['track'].unique()}")

# Find dogs by trainer
df_poulter = df[df['trainer'] == 'ADAM POULTER']
print(f"Dogs trained by Adam Poulter: {len(df_poulter)}")
```

### Access Specific Fields

```python
from src.parse_pdf_enhanced import parse_pdf_enhanced

dogs = parse_pdf_enhanced('forms/DRWN_2025-09-03.pdf')
dog = dogs[0]

# Basic information
print(f"Name: {dog['name']}")
print(f"Box: {dog['box']}")
print(f"Weight: {dog['weight']}kg")
print(f"Color: {dog['color']}")
print(f"Age: {dog['age']}")
print(f"Sex: {dog['sex']}")

# Pedigree
print(f"Sire: {dog['sire']}")
print(f"Dam: {dog['dam']}")

# Connections
print(f"Trainer: {dog['trainer']}")
print(f"Owner: {dog['owner']}")

# Performance metrics
print(f"API: {dog['api']}")
print(f"Career PM/start: {dog['car_pm_per_start']}")
print(f"Days since last start: {dog['dls']}")
print(f"Days since last win: {dog['dlw']}")

# Career statistics
print(f"Career record: {dog['horse_record']}")
print(f"Win%: {dog['horse_win_pct']}%")
print(f"Place%: {dog['horse_place_pct']}%")

# Race history
for i, race in enumerate(dog['race_history'], 1):
    print(f"Race {i}: {race[:100]}...")
```

## Output Formats

### CSV Format

The CSV output contains all 52 fields as columns. The `race_history` field is JSON-encoded as a list of strings.

```python
import pandas as pd

df = pd.read_csv('data/parsed.csv')
print(df.columns)  # See all available columns
print(df.head())   # View first few rows
```

### JSON Format

The JSON output preserves data types and stores race history as a proper list.

```python
import json

with open('data/parsed.json', 'r') as f:
    dogs = json.load(f)

# Access data
for dog in dogs:
    print(f"{dog['name']} - {len(dog['race_history'])} races")
```

## Command-Line Options

```bash
python src/parse_pdf_enhanced.py [OPTIONS]

Options:
  --forms DIR       Directory containing PDF forms (default: forms)
  --out FILE        Output CSV file (default: data/rns/parsed_enhanced.csv)
  --json FILE       Output JSON file (default: data/rns/parsed_enhanced.json)
  --sample          Process only first PDF for testing
```

## Extracted Fields

See [ENHANCED_PARSER_FIELDS.md](ENHANCED_PARSER_FIELDS.md) for complete field documentation including:

- Field descriptions and formats
- Example values
- Data quality notes
- Which fields may need additional parsing

## Performance

- **Speed**: ~0.5-1 second per PDF (depends on size)
- **Memory**: Processes PDFs one at a time
- **Accuracy**: 94%+ field completion for dogs with detailed sections

Test results:
- Successfully parsed 1,194 dogs from 19 PDFs
- Across 9 different tracks
- 74% of dogs have complete trainer information
- 73% of dogs have complete career statistics

## Comparison with Original Parser

| Feature | Original Parser | Enhanced Parser |
|---------|----------------|-----------------|
| Fields extracted | 5 | 52 |
| Race history | No | Yes (full detail) |
| Performance metrics | No | Yes (8+ metrics) |
| Career statistics | No | Yes |
| Grade statistics | No | Yes |
| Track conditions | No | Yes |
| Output formats | CSV | CSV + JSON |

## Common Use Cases

### 1. Analyze Trainer Performance

```python
import pandas as pd

df = pd.read_csv('data/parsed.csv')

# Group by trainer
trainer_stats = df.groupby('trainer').agg({
    'name': 'count',
    'horse_win_pct': 'mean',
    'api': 'mean'
}).sort_values('api', ascending=False)

print("Top trainers by average API:")
print(trainer_stats.head(10))
```

### 2. Find Dogs With Recent Wins

```python
import pandas as pd

df = pd.read_csv('data/parsed.csv')

# Dogs that won recently (DLW < 30 days)
recent_winners = df[df['dlw'].astype(float) < 30]
print(f"Dogs with wins in last 30 days: {len(recent_winners)}")
```

### 3. Export Race History for Analysis

```python
import json

with open('data/parsed.json', 'r') as f:
    dogs = json.load(f)

# Extract all race history
all_races = []
for dog in dogs:
    for race in dog['race_history']:
        all_races.append({
            'dog': dog['name'],
            'trainer': dog['trainer'],
            'race_detail': race
        })

print(f"Total race history entries: {len(all_races)}")
```

### 4. Compare Dogs in Same Race

```python
from src.parse_pdf_enhanced import parse_pdf_enhanced

dogs = parse_pdf_enhanced('forms/DRWN_2025-09-03.pdf')

# Get all dogs in race 1
race_1 = [d for d in dogs if d['race'] == 1]

print(f"Race 1 has {len(race_1)} dogs:")
for dog in race_1:
    print(f"  Box {dog['box']}: {dog['name']} - API {dog['api']}")
```

## Troubleshooting

### Missing Fields

Some dogs may have missing fields if:
- They don't have a detailed section in the PDF (only appear in summary)
- The information is not available (e.g., no wins yet)
- PDF formatting issues

Check the `None` and empty string values in the output.

### PDF Format Issues

If a PDF doesn't parse correctly:
1. Check the PDF filename format (should be `TRACK_YYYY-MM-DD.pdf`)
2. Verify it's a Racing & Sports form guide PDF
3. Check the PDF text extraction with:

```python
from pdfminer.high_level import extract_text
text = extract_text('forms/PROBLEM.pdf')
print(text[:1000])  # Check if text is readable
```

### Performance Issues

For large batches of PDFs:
- Process in smaller batches
- Use `--sample` flag for testing
- Monitor memory usage

## Development

### Adding New Fields

To extract additional fields:

1. Identify the field in the PDF text
2. Add field to `dog_info` dict in `parse_dog_details()`
3. Add parsing logic in appropriate section
4. Update documentation

### Testing

```bash
# Test on single PDF
python src/parse_pdf_enhanced.py --forms forms --sample

# Run demonstration
python demo_extraction.py

# Verify field extraction
python -c "from src.parse_pdf_enhanced import parse_pdf_enhanced; \
  dogs = parse_pdf_enhanced('forms/DRWN_2025-09-03.pdf'); \
  print(f'Extracted {len(dogs)} dogs'); \
  print(f'Fields: {list(dogs[0].keys())}')"
```

## Support

For issues or questions:
1. Check [ENHANCED_PARSER_FIELDS.md](ENHANCED_PARSER_FIELDS.md) for field documentation
2. Run `demo_extraction.py` to see working example
3. Review the code comments in `src/parse_pdf_enhanced.py`

## License

Same as parent project.

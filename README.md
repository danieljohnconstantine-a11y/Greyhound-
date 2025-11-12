# Mastercontrol Greyhound

A fully automated, free, and open-source Python system for ingesting daily Australian greyhound PDF race forms.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full automation pipeline
python src/mastercontrol.py

# View outputs
cat data/output/summary.md
head data/output/probabilities.csv
```

## Overview

This system automatically:
- Discovers and downloads daily greyhound race form PDFs from Racing & Sports
- Parses PDF content to extract race information, runners, and box numbers
- Generates structured data outputs (CSV format)
- Provides probability calculations and race summaries
- Runs on a scheduled basis via GitHub Actions

## Features

- **Automated PDF Discovery**: Crawls Racing & Sports website to find current day's greyhound meeting PDFs
- **Robust Downloading**: Implements retry logic with exponential backoff for reliable PDF fetching
- **PDF Parsing**: Extracts structured race data from PDF forms using pdfminer.six
- **Data Pipeline**: End-to-end processing from PDF download to structured CSV output
- **Free & Open Source**: MIT licensed, runs on free GitHub Actions infrastructure
- **Australian Time Zone Support**: Correctly handles AEST/AEDT timezone calculations
- **Multiple Track Support**: Covers major Australian greyhound tracks (VIC, NSW, QLD, SA, NT)

## Installation

### Prerequisites
- Python 3.11 or higher
- pip package manager

### Setup

1. Clone the repository:
```bash
git clone https://github.com/danieljohnconstantine-a11y/Greyhound-.git
cd Greyhound-
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Recommended: Use Mastercontrol Script

The easiest way to run the complete pipeline:

```bash
# Run full pipeline for today (fetch + parse + report)
python src/mastercontrol.py

# Run for a specific date
python src/mastercontrol.py --date 2025-09-01

# Skip fetching, only parse existing PDFs
python src/mastercontrol.py --skip-fetch

# Enable verbose output for debugging
python src/mastercontrol.py --verbose
```

**Output files:**
- `data/output/parsed_YYYYMMDDTHHMMSSZ.csv` - Timestamped parsed data
- `data/output/probabilities.csv` - Win probabilities for each runner
- `data/output/summary.md` - Human-readable race summary

### Manual Execution (Advanced)

#### Fetch Forms
Download today's race form PDFs:
```bash
python src/fetch_forms.py --out-dir data/input
```

Or use the standalone scraper:
```bash
python scraper.py
```

#### Parse PDFs
Extract structured data from downloaded PDFs:
```bash
python src/parse_pdf.py --forms data/input --out data/output/parsed.csv
```

#### Full Daily Pipeline
Run the complete pipeline (fetch + parse + report):
```bash
python src/run_daily_html.py
```

### Automated Execution

The system runs automatically via GitHub Actions:
- **Schedule**: Multiple times daily (6:05, 7:05, 8:05, 9:05, 10:05 AM AEST)
- **Workflow**: `.github/workflows/greyhound_html.yml`
- **Output**: Results committed to repository under `reports/latest/`

## Project Structure

```
mastercontrol-greyhound/
├── src/                      # Source code modules
│   ├── fetch_forms.py       # PDF downloading logic
│   ├── parse_pdf.py         # PDF parsing and data extraction
│   ├── run_daily.py         # Daily pipeline orchestration
│   ├── run_daily_html.py    # HTML-based pipeline
│   ├── html_fetch.py        # HTML scraping utilities
│   ├── http_client.py       # HTTP client with retry logic
│   └── utils.py             # Utility functions
├── data/
│   ├── input/               # Downloaded PDF forms (gitignored)
│   ├── output/              # Parsed data outputs
│   ├── rns/                 # Racing & Sports data
│   └── html/                # HTML scraping cache
├── forms/                   # PDF storage directory
├── reports/                 # Generated reports
│   └── latest/              # Latest run outputs
│       ├── probabilities.csv
│       └── summary.md
├── tests/                   # Test suite
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── LICENSE                 # MIT License
└── .gitignore              # Git ignore rules
```

## Supported Tracks

The system currently supports the following Australian greyhound tracks:
- **VIC**: Sale (SALE), Healesville (HEAL)
- **NSW**: Richmond (RICH), Grafton (GRAF)
- **QLD**: Capalaba (CAPA), QLD Straight (QSTR)
- **SA**: Gawler (GAWL)
- **NT**: Darwin (DRWN)

Additional tracks can be added by updating `TRACK_CODES` in `scraper.py`.

## Data Output Format

### Parsed CSV Structure
```csv
track,date,race,box,runner
SALE,2025-09-01,1,1,FAST THUNDER
SALE,2025-09-01,1,2,QUICK STAR
...
```

### Probabilities CSV
```csv
track,date,race,box,runner,prob_win
SALE,2025-09-01,1,1,FAST THUNDER,0.125
...
```

## Configuration

### Environment Variables
- `FORCE_DATE`: Override date (format: YYYY-MM-DD) for testing
- `TZ`: Timezone setting (default: Australia/Brisbane)

### Track Codes
Modify `TRACK_CODES` in `scraper.py` to add or remove tracks.

## Development

### Running Tests
```bash
python -m pytest tests/
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## Technical Details

### PDF Processing
- Uses `pdfminer.six` for text extraction
- Implements regex patterns to identify race numbers and runner names
- Handles various PDF formats from Racing & Sports

### HTTP Client
- Custom retry logic with exponential backoff
- Handles rate limiting (403, 429 status codes)
- Polite crawling with configurable delays
- User-agent rotation for reliability

### Data Quality
- Validates PDF content before saving (magic byte checking)
- Minimum file size validation (>12KB for forms)
- Content-type verification
- Deduplication of URLs

## Troubleshooting

### No PDFs Downloaded
- Check internet connectivity
- Verify Racing & Sports website is accessible
- Check if meetings are scheduled for the current date
- Review GitHub Actions logs for detailed error messages

### Parsing Errors
- Ensure PDFs are valid and not corrupted
- Check PDF format matches expected structure
- Update regex patterns if R&S changes PDF format

### GitHub Actions Failures
- Check action logs in the "Actions" tab
- Verify dependencies are correctly installed
- Ensure repository secrets are configured (if needed)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Racing & Sports for providing race form data
- GitHub Actions for free automation infrastructure
- Open source community for excellent Python libraries

## Disclaimer

This tool is for educational and research purposes. Users are responsible for ensuring compliance with Racing & Sports terms of service and applicable data usage regulations.


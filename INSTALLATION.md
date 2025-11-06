# Installation Guide

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

## Installation Steps

### 1. Install Required Python Packages

Run the following command in the repository root directory:

```bash
pip install -r requirements.txt
```

Or install packages individually:

```bash
pip install pandas pdfplumber openpyxl requests beautifulsoup4 lxml python-dateutil pytz tenacity
```

### 2. Verify Installation

Test that the installation was successful:

```bash
python main.py --help
```

If you see the help message or the script runs without import errors, you're ready to go!

## Quick Start

### For Basic 11-Field Extraction

```bash
# Add PDF files to the data directory
python main.py
```

Output files will be in the `/outputs` directory:
- `greyhound_analysis_<timestamp>.csv`
- `greyhound_analysis_<timestamp>.xlsx`

### For Enhanced 62-Field Extraction (Recommended)

```bash
# Add PDF files to the data directory
python main_enhanced.py
```

Output files will be in the `/outputs` directory:
- `greyhound_full_analysis_<timestamp>.csv`
- `greyhound_full_analysis_<timestamp>.xlsx`

## Troubleshooting

### ModuleNotFoundError

If you see an error like:
```
ModuleNotFoundError: No module named 'pandas'
```

Make sure you've installed the requirements:
```bash
pip install -r requirements.txt
```

### No PDFs Found

If you see "No data extracted from PDFs", make sure:
1. PDF files are placed in the `/data` directory (or subdirectories)
2. The PDF files have a `.pdf` extension
3. The PDFs contain greyhound race form data

## Directory Structure

```
Greyhound-/
├── data/              # Place your PDF files here
│   └── ...
├── outputs/           # Generated reports appear here
│   ├── greyhound_analysis_*.csv
│   ├── greyhound_analysis_*.xlsx
│   └── parse.log
├── src/               # Parser source code
├── main.py            # Basic 11-field parser
└── main_enhanced.py   # Enhanced 62-field parser
```

## Support

For issues or questions, please refer to:
- `PIPELINE_README.md` - Detailed usage documentation
- `IDENTIFIED_FIELDS.md` - Field extraction details

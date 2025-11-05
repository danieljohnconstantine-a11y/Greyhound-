# Greyhound Form Guide Processor

Automated greyhound racing form guide processing system. Fetches, parses, and analyzes race forms from Racing & Sports Australia.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run complete pipeline for today
python main.py

# Run daily processing (Windows)
run_main.bat

# Run tests
python test_parser.py
```

## Project Structure

- `main.py` - Main entry point for processing
- `run_daily.py` - Daily wrapper script
- `debug_parser.py` - Parser debugging utility
- `test_parser.py` - Parser test suite
- `parser/` - PDF parsing module
- `data/` - Raw data storage
- `outputs/` - Generated reports and predictions
- `src/` - Additional processing modules

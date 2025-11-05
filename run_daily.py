#!/usr/bin/env python3
"""
run_daily.py - Daily runner script

Simple wrapper to run the main processing pipeline for today's races.
Fetches forms, parses them, and generates reports.
"""

import sys
import subprocess
from datetime import datetime


def main():
    """Run daily processing."""
    print(f"Starting daily greyhound processing - {datetime.now()}")
    
    # Run main.py without date argument (uses today)
    result = subprocess.run([sys.executable, "main.py"], check=False)
    
    if result.returncode != 0:
        print(f"Processing failed with exit code {result.returncode}")
        return result.returncode
    
    print("Daily processing completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

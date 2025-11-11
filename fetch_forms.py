#!/usr/bin/env python3
"""
fetch_forms.py
Download greyhound racing form PDFs from Racing & Sports.
This module provides helper functions for run_daily.py integration.
"""

import os
import sys
import datetime
import time
from pathlib import Path
from datetime import datetime as dt, timezone
import requests
from dateutil import tz


# Australian Eastern timezone
SYDNEY_TZ = tz.gettz("Australia/Sydney")


def sydney_today() -> str:
    """Return today's date in Sydney timezone as YYYY-MM-DD string."""
    now_utc = dt.now(timezone.utc)
    syd = now_utc.astimezone(SYDNEY_TZ)
    return syd.strftime("%Y-%m-%d")


def fetch_for_date(date_str: str, out_dir: str) -> dict:
    """
    Fetch greyhound forms for a specific date.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        out_dir: Output directory for downloaded PDFs
    
    Returns:
        Dictionary mapping track codes to lists of downloaded files
    """
    outdir = Path(out_dir)
    outdir.mkdir(exist_ok=True, parents=True)
    
    print(f"[fetch] Fetching forms for {date_str}...")
    
    # Use the comprehensive fetch_all from src.fetch_forms
    try:
        # Import the working implementation
        sys.path.insert(0, str(Path(__file__).parent / "src"))
        from fetch_forms import fetch_all as _fetch_all
        
        saved_count = _fetch_all(outdir)
        
        if saved_count == 0:
            print("[WARN] No forms downloaded! Check if Racing & Sports has data for this date.")
            print(f"[WARN] Looking for greyhound forms on {date_str}")
            print("[WARN] This is normal if:")
            print("       - No races scheduled today")
            print("       - Forms not yet published")
            print("       - Website structure changed")
            return {}
        
        # Build result dictionary grouped by track code
        result = {}
        for pdf_file in outdir.glob("*.pdf"):
            # Extract track code from filename (format: CODE_YYYY-MM-DD.pdf)
            name = pdf_file.stem
            parts = name.split("_")
            if len(parts) >= 1:
                track_code = parts[0]
                if track_code not in result:
                    result[track_code] = []
                result[track_code].append(str(pdf_file))
                print(f"[fetch] Downloaded: {pdf_file.name} (Track: {track_code})")
        
        print(f"[fetch] Successfully downloaded {saved_count} form(s) for {len(result)} track(s)")
        return result
        
    except ImportError as e:
        print(f"[ERROR] Could not import fetch_all from src.fetch_forms: {e}")
        print("[ERROR] Falling back to basic fetch...")
        return _basic_fetch(date_str, outdir)
    except Exception as e:
        print(f"[ERROR] Failed to fetch forms: {e}")
        import traceback
        traceback.print_exc()
        return {}


def _basic_fetch(date_str: str, outdir: Path) -> dict:
    """
    Basic fallback fetch implementation.
    Attempts to download from common Racing & Sports URLs.
    """
    # Common Australian greyhound track codes
    TRACK_CODES = [
        "RICH", "SALE", "GAWL", "CAPA", "WARR", "TARA", "DBOW", 
        "SAND", "BRIS", "ALBI", "MELB", "GEELONG", "BALL", "BURY"
    ]
    
    BASE_URL = "https://www.racingandsports.com.au"
    
    result = {}
    saved_count = 0
    
    for track_code in TRACK_CODES:
        # Try various URL patterns that Racing & Sports uses
        urls_to_try = [
            f"{BASE_URL}/form-guide/greyhound-racing/{track_code.lower()}/{date_str}/",
            f"{BASE_URL}/form-guide/greyhound/{track_code.lower()}/{date_str}/",
        ]
        
        for url in urls_to_try:
            try:
                print(f"[fetch] Trying {track_code}: {url}")
                resp = requests.get(url, timeout=15, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                })
                
                if resp.status_code == 200:
                    # Page exists, now look for PDF links
                    # This is simplified - real implementation would parse HTML
                    print(f"[fetch] Found page for {track_code}")
                    # Would need to parse HTML and extract PDF links here
                    # For now, just note success
                    
                time.sleep(1)  # Be polite to the server
                break
                    
            except Exception as e:
                print(f"[fetch] Error fetching {track_code}: {e}")
                continue
    
    if saved_count == 0:
        print("[WARN] Basic fetch found no forms. Install beautifulsoup4 for full functionality:")
        print("       pip install beautifulsoup4 lxml")
    
    return result


def main():
    """Command-line interface for testing."""
    import argparse
    ap = argparse.ArgumentParser(description="Fetch greyhound racing forms")
    ap.add_argument("--date", default=None, help="Date in YYYY-MM-DD format (default: today)")
    ap.add_argument("--out-dir", default="forms", help="Output directory")
    args = ap.parse_args()
    
    date_str = args.date or sydney_today()
    result = fetch_for_date(date_str, args.out_dir)
    
    print(f"\n[fetch] Summary:")
    print(f"  Date: {date_str}")
    print(f"  Tracks: {len(result)}")
    print(f"  Total files: {sum(len(v) for v in result.values())}")
    
    return 0 if result else 1


if __name__ == "__main__":
    sys.exit(main())

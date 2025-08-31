#!/usr/bin/env python3
"""
R&S Greyhound Form PDF Scraper (Ladbrokes tracks)
- Builds the known R&S PDF URLs for a given date and venue codes
- Downloads available PDFs
- Saves to data/forms/YYYYMMDD/<VENUECODE>.pdf
- Emits a manifest JSON of successes/failures

Usage:
  python scraper.py                # uses today's date (AEST) and all default venues
  python scraper.py --date 2025-08-31
  python scraper.py --date 20250831 --venues RICHG HEALG DRWNG
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from zoneinfo import ZoneInfo

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

# Known Ladbrokes-listed venue codes (Racing & Sports PDF prefix)
# Add/remove as needed.
VENUE_CODES: Dict[str, str] = {
    # NSW
    "RICHG": "Richmond",
    "GRAFG": "Grafton",
    "DRWNG": "Dapto (Druin/Dapto Wednesday card on R&S short-code DRWNG)",
    "HEALG": "Healesville (Vic; straight track but appears in Ladbrokes)",
    # VIC
    "SALEG": "Sale",
    # SA
    "GAWLG": "Gawler",
    # QLD
    "QSTRG": "Queensland straight (Qld regional; appears on R&S with this code)",
    # TAS / NT (add if needed)
    # "HOBTG": "Hobart",
    # "LAUCT": "Launceston",
    # Others we saw earlier; add as you confirm they appear on Ladbrokes:
    "CAPAG": "Capalaba",
    # If you want to try Wyong/Taree equivalents etc., add here when you confirm PDF codes:
    # "WYNGT": "Wyong (if greyhounds meet appears on R&S)",
}

RNS_BASE = "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf"

# Networking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GreyhoundFormBot/1.0; +https://github.com/yourrepo)",
    "Accept": "application/pdf, */*",
}
TIMEOUT = 20  # seconds
RETRY = 2     # simple retries per file
SLEEP_BETWEEN = 0.7  # be polite


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def parse_date_arg(date_str: str | None) -> dt.date:
    """Parse --date argument. Accepts YYYY-MM-DD or YYYYMMDD. Defaults to 'today' in AEST."""
    if not date_str:
        # Today in AEST because Aus meetings are local to Eastern time most days.
        today_aest = dt.datetime.now(ZoneInfo("Australia/Sydney")).date()
        return today_aest
    # Try YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    # Try YYYYMMDD
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", date_str)
    if m:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    raise ValueError("Invalid --date format. Use YYYY-MM-DD or YYYYMMDD.")


def ddmm(date_obj: dt.date) -> str:
    """Return DDMM string for the R&S filename convention."""
    return date_obj.strftime("%d%m")


def build_pdf_url(venue_code: str, date_obj: dt.date) -> str:
    """
    R&S file naming convention:
      {VENUE}{DDMM}form.pdf  (e.g., SALEG3108form.pdf)
    """
    return f"{RNS_BASE}/{venue_code}{ddmm(date_obj)}form.pdf"


def ensure_dir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)


def fetch_pdf(url: str) -> Tuple[bool, bytes | None, str | None]:
    """Try to fetch the PDF. Returns (ok, content, error_message)."""
    for attempt in range(1, RETRY + 2):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code == 200 and resp.headers.get("Content-Type", "").lower().startswith("application/pdf"):
                return True, resp.content, None
            elif resp.status_code == 404:
                return False, None, "Not found (404)"
            else:
                err = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            err = f"Request error: {e}"
        if attempt <= RETRY:
            time.sleep(1.2 * attempt)
            continue
        return False, None, err


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download R&S Greyhound form PDFs for Ladbrokes-listed tracks.")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD or YYYYMMDD). Defaults to today (AEST).", default=None)
    parser.add_argument("--venues", nargs="*", help="Optional list of venue codes to limit (e.g., RICHG HEALG).")
    parser.add_argument("--out", default="data/forms", help="Output root directory (default: data/forms)")
    args = parser.parse_args()

    target_date = parse_date_arg(args.date)
    ymd = target_date.strftime("%Y%m%d")
    out_root = Path(args.out) / ymd
    ensure_dir(out_root)

    if args.venues:
        # Validate user-specified list against our known mapping
        missing = [v for v in args.venues if v not in VENUE_CODES]
        if missing:
            print(f"[warn] Unknown venue codes (skipped): {', '.join(missing)}")
        venues = [v for v in args.venues if v in VENUE_CODES]
    else:
        venues = list(VENUE_CODES.keys())

    print(f"Date: {target_date}  (AEST)")
    print(f"Saving to: {out_root.resolve()}")
    print(f"Venues: {', '.join(venues)}")
    print("-" * 60)

    manifest = {
        "date": ymd,
        "venues": {},
    }

    for code in venues:
        url = build_pdf_url(code, target_date)
        nice = VENUE_CODES.get(code, code)
        print(f"[{code}] {nice} -> {url}")
        ok, content, err = fetch_pdf(url)
        if ok and content:
            outfile = out_root / f"{code}.pdf"
            outfile.write_bytes(content)
            size_kb = len(content) / 1024
            print(f"   ✓ saved {outfile.name} ({size_kb:.1f} KB)")
            manifest["venues"][code] = {
                "name": nice,
                "url": url,
                "saved": str(outfile),
                "status": "ok",
                "size_kb": round(size_kb, 1),
            }
        else:
            print(f"   ✗ failed: {err}")
            manifest["venues"][code] = {
                "name": nice,
                "url": url,
                "saved": None,
                "status": "error",
                "error": err,
            }
        time.sleep(SLEEP_BETWEEN)

    # Write manifest
    mf_path = out_root / "manifest.json"
    mf_path.write_text(json.dumps(manifest, indent=2))
    print("-" * 60)
    print(f"Manifest written: {mf_path.resolve()}")

    # Quick summary
    ok_count = sum(1 for v in manifest["venues"].values() if v["status"] == "ok")
    err_count = sum(1 for v in manifest["venues"].values() if v["status"] != "ok")
    print(f"Done. OK: {ok_count}  Failed: {err_count}")


if __name__ == "__main__":
    main()

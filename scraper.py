#!/usr/bin/env python3
"""
scraper.py
-----------
Downloads Racing & Sports form PDFs for today's (AEST/AEDT) greyhound meetings
that appear on Ladbrokes frequently, saves them under /forms with friendly
filenames like SALE_2025-09-01.pdf, and prints a short summary.

You can force a specific date by setting env var FORCE_DATE=YYYY-MM-DD
(e.g. in GitHub Actions workflow_dispatch input).

Only standard library + 'requests' are used.
"""

from __future__ import annotations

import os
import sys
import time
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import requests


# ------------------------------ Config --------------------------------------

# Racing & Sports base URL for PDF form guides
BASE_URL = "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf"

# Tracks we care about (Ladbrokes). Add/remove codes as needed.
# The code format is <VENUE><G>, and URL format is {CODE}{DDMM}form.pdf
TRACK_CODES = [
    # VIC
    "SALEG",     # Sale
    "HEALG",     # Healesville
    # NSW
    "RICHG",     # Richmond
    "GRAFG",     # Grafton
    # QLD
    "CAPAG",     # Capalaba
    "QSTRG",     # Qld Straight (if listed on the day)
    # SA
    "GAWLG",     # Gawler
    # NT
    "DRWNG",     # Darwin
    # Add more as you need (check the actual code used on R&S):
    # "ALBGG", "DAPTG", "GOSFG", "GEELG", "BENDG", etc.
]

# Nice short names for filenames
SHORT_NAME: Dict[str, str] = {
    "SALEG": "SALE",
    "HEALG": "HEAL",
    "RICHG": "RICH",
    "GRAFG": "GRAF",
    "CAPAG": "CAPA",
    "QSTRG": "QSTR",
    "GAWLG": "GAWL",
    "DRWNG": "DRWN",
}

# Where to save PDFs
OUT_DIR = Path("forms")


# ------------------------------ Helpers -------------------------------------

def au_today() -> datetime:
    """
    Return 'today' in AUS east time (AEST/AEDT).
    For our purpose, a simple UTC+10 is sufficient.
    If you want DST-aware, use zoneinfo('Australia/Sydney').
    """
    return datetime.now(timezone.utc) + timedelta(hours=10)


def date_tokens(dt: datetime) -> Tuple[str, str]:
    """
    Returns (ddmm, yyyy-mm-dd) tokens for URL and filename.
    """
    return dt.strftime("%d%m"), dt.strftime("%Y-%m-%d")


def short_from_code(code: str) -> str:
    return SHORT_NAME.get(code, code[:4])


def build_url(code: str, ddmm: str) -> str:
    return f"{BASE_URL}/{code}{ddmm}form.pdf"


@dataclass
class FetchResult:
    code: str
    short: str
    url: str
    path: Path
    ok: bool
    status: int | None
    bytes: int | None
    error: str | None


def http_get_with_retries(url: str, *, max_tries: int = 3, timeout: int = 30) -> requests.Response:
    """
    GET with basic retry + jitter on 5xx/timeouts/connection errors.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; GreyhoundScraper/1.0; +https://github.com/)",
        "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }

    last_exc: Exception | None = None
    for attempt in range(1, max_tries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            return resp
        except requests.RequestException as e:
            last_exc = e
            sleep_s = min(8, 1.5 ** attempt) + random.uniform(0, 0.4)
            print(f"   … network error on try {attempt}/{max_tries}: {e}. Retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    # If we’re here, all attempts failed
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown error in http_get_with_retries")


def fetch_one(code: str, dt: datetime) -> FetchResult:
    ddmm, ymd = date_tokens(dt)
    url = build_url(code, ddmm)
    short = short_from_code(code)
    OUT_DIR.mkdir(exist_ok=True)
    out_path = OUT_DIR / f"{short}_{ymd}.pdf"

    try:
        print(f"→ Fetching {code}: {url}")
        resp = http_get_with_retries(url)
        status = resp.status_code

        if status == 200 and resp.headers.get("Content-Type", "").lower().startswith("application/pdf"):
            data = resp.content
            out_path.write_bytes(data)
            print(f"   ✓ Saved {out_path.name} ({len(data):,} bytes)")
            return FetchResult(code, short, url, out_path, True, status, len(data), None)

        # Handle 404 (no meeting) clearly
        if status == 404:
            print(f"   • No PDF (404). Likely no meeting for {code} today.")
            return FetchResult(code, short, url, out_path, False, status, None, "404 Not Found")

        # All other non-200s
        print(f"   ✗ HTTP {status}. Content-Type={resp.headers.get('Content-Type')}")
        return FetchResult(code, short, url, out_path, False, status, None, f"HTTP {status}")

    except Exception as e:
        return FetchResult(code, short, url, out_path, False, None, None, str(e))


# ------------------------------ Main ----------------------------------------

def main() -> int:
    # Optional: FORCE_DATE=YYYY-MM-DD  (useful for manual/workflow testing)
    force = os.getenv("FORCE_DATE", "").strip()
    if force:
        try:
            dt = datetime.strptime(force, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            print(f"Using FORCE_DATE={force} (interpreted as UTC midnight)")
        except ValueError:
            print("ERROR: FORCE_DATE must be YYYY-MM-DD, e.g. 2025-09-01")
            return 2
    else:
        dt = au_today()
        print(f"Using AUS-east 'today': {dt.strftime('%Y-%m-%d')} (UTC+10 approx)")

    ok_results: List[FetchResult] = []
    bad_results: List[FetchResult] = []

    for code in TRACK_CODES:
        res = fetch_one(code, dt)
        (ok_results if res.ok else bad_results).append(res)
        # polite pacing to avoid hammering the server
        time.sleep(0.6)

    # Summary
    print("\n================ Summary ================")
    if ok_results:
        print("Downloaded:")
        for r in ok_results:
            sz = f"{r.bytes:,}B" if r.bytes is not None else "-"
            print(f"  - {r.path.name}  ({sz})")
    else:
        print("Downloaded: none")

    if bad_results:
        print("\nSkipped / Failed:")
        for r in bad_results:
            reason = r.error or f"HTTP {r.status}"
            print(f"  - {r.code}: {reason}")

    print("========================================\n")

    # Exit 0 even if some failed (common when certain tracks don’t race daily).
    return 0


if __name__ == "__main__":
    sys.exit(main())

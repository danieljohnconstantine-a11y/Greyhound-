#!/usr/bin/env python3
"""
Download Racing and Sports greyhound form PDFs and save with short track codes.

- No external dependencies (uses urllib).
- Auto-creates a 'forms' folder.
- Auto-names from URL (e.g., SALEG3108form.pdf -> SALE.pdf).
- Avoids overwriting by appending _2, _3, ... when needed.
"""

import os
import re
import sys
import time
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ------------------------------------------------------------
# 1) Add today's URLs here (you can edit this list daily or
#    wire it to your GitHub Automation later).
# ------------------------------------------------------------
URLS = [
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/HEALG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/CAPAG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/DRWNG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/GRAFG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/GAWLG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/QSTRG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/RICHG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/SALEG3108form.pdf",
]

OUTPUT_DIR = "forms"
TIMEOUT_SEC = 30
USER_AGENT = "Mozilla/5.0 (compatible; GreyhoundFormBot/1.0; +https://github.com/)"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def basename_from_url(url: str) -> str:
    return os.path.basename(urlparse(url).path)

def short_track_code(filename: str) -> str:
    """
    From 'SALEG3108form.pdf' -> 'SALE'
    Logic:
      - take the leading letters from the start of the filename (e.g., 'SALEG')
      - return the first 4 uppercase letters
      - fallback: use the first 4 characters of the filename
    """
    m = re.match(r"([A-Za-z]+)", filename)
    if m:
        letters = m.group(1).upper()
        if len(letters) >= 4:
            return letters[:4]
        if letters:
            return letters  # shorter than 4, but still something
    # Fallback
    return filename[:4].upper()

def unique_path(base_path: str) -> str:
    """
    If base_path exists, append _2, _3, ... to get a unique path.
    """
    if not os.path.exists(base_path):
        return base_path
    root, ext = os.path.splitext(base_path)
    i = 2
    while True:
        candidate = f"{root}_{i}{ext}"
        if not os.path.exists(candidate):
            return candidate
        i += 1

def download_pdf(url: str, out_path: str) -> bool:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=TIMEOUT_SEC) as resp:
            data = resp.read()
    except HTTPError as e:
        print(f"[ERROR] HTTP {e.code} fetching {url}")
        return False
    except URLError as e:
        print(f"[ERROR] URL error fetching {url}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error fetching {url}: {e}")
        return False

    with open(out_path, "wb") as f:
        f.write(data)
    return True

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main() -> int:
    ensure_dir(OUTPUT_DIR)

    saved = 0
    failed = 0

    print(f"Found {len(URLS)} URLs to fetch.\n")

    for url in URLS:
        raw_name = basename_from_url(url)                # e.g., SALEG3108form.pdf
        code = short_track_code(raw_name)                # e.g., SALE
        short_name = f"{code}.pdf"                       # e.g., SALE.pdf
        final_path = unique_path(os.path.join(OUTPUT_DIR, short_name))

        print(f"- Fetching: {url}")
        print(f"  → Will save as: {os.path.basename(final_path)}")

        ok = download_pdf(url, final_path)
        if ok:
            saved += 1
            print(f"  ✔ Saved to {final_path}\n")
        else:
            failed += 1
            print(f"  ✖ Failed\n")

        # Be polite to the host
        time.sleep(1)

    print("----- Summary -----")
    print(f"Saved:  {saved}")
    print(f"Failed: {failed}")
    print(f"Output folder: {OUTPUT_DIR}")

    # Exit code 0 even on some failures, so the workflow can still commit partial results.
    return 0

if __name__ == "__main__":
    sys.exit(main())

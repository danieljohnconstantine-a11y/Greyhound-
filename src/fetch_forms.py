#!/usr/bin/env python3
"""
Fetch greyhound form PDFs reliably by:
1) Discovering live Racing & Sports 'newformpdf' links from their listings page.
2) Falling back to probing a few known URL patterns for known track codes.
3) Writing any successful PDFs into ./forms as {TRACK}_{YYYY-MM-DD}.pdf

This removes guesswork and dramatically reduces 404s.
"""

from __future__ import annotations
import os
import re
import sys
import time
import json
import math
import shutil
import random
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Tuple, Dict, Set, Optional

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
LISTING_URLS = [
    # Racing & Sports greyhound listings (Australia). We try a couple of likely pages.
    # If R&S ever changes structure, we still have the brute pattern probe fallback below.
    "https://www.racingandsports.com.au/form-guide/greyhound/australia",
    "https://www.racingandsports.com.au/form-guide/greyhound"  # backup
]

# Known good CDN host where PDFs live:
CDN_HOST_SUBSTR = "files.racingandsports.com/racing/raceinfo/newformpdf"

# Where to write PDFs (repo root relative)
FORMS_DIR = os.path.join(os.path.dirname(__file__), "..", "forms")

# AU time (most PDFs appear daytime local)
AUS_TZ = timezone(timedelta(hours=10))  # AEST standard offset; good enough for daily naming
TODAY = datetime.now(AUS_TZ).date()
YMD = TODAY.isoformat()  # YYYY-MM-DD
DDMM = TODAY.strftime("%d%m")
DDMMYY = TODAY.strftime("%d%m%y")

# We’ll reuse the track codes you already have in /forms and supplement with a default set
DEFAULT_TRACK_CODES = [
    "ALBN", "ANGLE", "BALL", "BEND", "BULL", "CAPA", "CANN", "CASI", "CRAN", "DAPTO",
    "DRWN", "GAWL", "GEEL", "GRAF", "GOSF", "HEAL", "HOBT", "IPSW", "LAUN",
    "MAIT", "MAND", "MURR", "QSTR", "RICH", "ROCK", "SAND", "SHEP", "TARE",
    "TOWN", "TRAR", "WENT", "WARR", "WAGG", "BATH", "GUNN", "LITH", "ALBU",
    "MTGA", "CANNON", "ANGLEPARK", # some alternates seen historically; kept in case
]

# URL patterns occasionally used historically by R&S. We’ll probe each per track code:
PATTERNS = [
    # Classic: TRACK + DDMM + 'form.pdf'
    "https://files.racingandsports.com/racing/raceinfo/newformpdf/{track}{ddmm}form.pdf",
    # Sometimes an underscore creeps in via site download links:
    "https://files.racingandsports.com/racing/raceinfo/newformpdf/{track}_{ddmm}form.pdf",
    # Some sites embed YY:
    "https://files.racingandsports.com/racing/raceinfo/newformpdf/{track}{ddmmyy}form.pdf",
    # Very rarely hyphenated:
    "https://files.racingandsports.com/racing/raceinfo/newformpdf/{track}-{ddmm}form.pdf",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.racingandsports.com.au/"
}

PDF_HEADERS = {
    **HEADERS,
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
}

RETRY = 3
TIMEOUT = 20


def log_setup():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_known_tracks_from_forms(forms_dir: str) -> Set[str]:
    """Infer track codes from existing files like CANN_2025-09-07.pdf."""
    if not os.path.isdir(forms_dir):
        return set()
    codes: Set[str] = set()
    for name in os.listdir(forms_dir):
        if not name.lower().endswith(".pdf"):
            continue
        m = re.match(r"([A-Z]+)_[0-9]{4}-[0-9]{2}-[0-9]{2}\.pdf$", name)
        if m:
            codes.add(m.group(1))
    return codes


def http_get(url: str, headers: dict, timeout: int = TIMEOUT) -> requests.Response:
    last_exc = None
    for attempt in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code in (200, 206):
                return r
            # For 403/404, wait a tad and retry
            time.sleep(0.9 * attempt)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(1.2 * attempt)
    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"Failed to GET {url} after {RETRY} attempts")


def discover_pdf_links() -> Dict[str, str]:
    """
    Hit one or two R&S listing pages and return map {track_code: pdf_url}.
    We look for any <a href> containing the 'newformpdf' CDN and
    derive a sensible track code from the filename prefix.
    """
    found: Dict[str, str] = {}
    for listing in LISTING_URLS:
        try:
            resp = http_get(listing, HEADERS)
        except Exception:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if CDN_HOST_SUBSTR not in href:
                continue
            # Normalize absolute
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.racingandsports.com.au" + href

            # Extract something that looks like TRACK + date + 'form.pdf'
            file = href.split("/")[-1]
            # Examples we’ve seen:
            #   CANN0609form.pdf
            #   CANN_0609form.pdf
            #   RICH1309form.pdf
            m = re.match(r"([A-Za-z]+)[_\-]?(\d{4,6})form\.pdf", file)
            if not m:
                # last-ditch: keep it but call it UNKNOWN
                track = "UNK"
            else:
                track = m.group(1).upper()

            # Keep first seen per track
            found.setdefault(track, href)
    return found


def save_pdf(url: str, track: str, ymd: str) -> Optional[str]:
    try:
        r = http_get(url, PDF_HEADERS)
        if "application/pdf" not in r.headers.get("Content-Type", "").lower() and len(r.content) < 10_000:
            # Likely an error doc
            return None
        out_name = f"{track}_{ymd}.pdf"
        out_path = os.path.join(FORMS_DIR, out_name)
        with open(out_path, "wb") as f:
            f.write(r.content)
        return out_path
    except Exception:
        return None


def try_bruteforce_patterns(track_codes: Iterable[str]) -> Dict[str, str]:
    """
    Probe the historical patterns for each track; return {track: saved_path} for successes.
    """
    saved: Dict[str, str] = {}
    for code in track_codes:
        for pat in PATTERNS:
            url = pat.format(track=code, ddmm=DDMM, ddmmyy=DDMMYY)
            path = save_pdf(url, code, YMD)
            if path:
                saved[code] = path
                break  # next track
    return saved


def main():
    log_setup()
    ensure_dir(FORMS_DIR)

    # 1) Discover live PDF links from listing page(s)
    logging.info("Discovering live PDF links from R&S listings…")
    discovered = discover_pdf_links()
    logging.info("Discovered %d CDN links", len(discovered))

    wrote: Dict[str, str] = {}
    for track, url in discovered.items():
        path = save_pdf(url, track, YMD)
        if path:
            wrote[track] = path

    # 2) If we’re still thin on files, probe patterns for known track codes (using history + defaults)
    if len(wrote) < 4:  # threshold; tweak as needed
        known = get_known_tracks_from_forms(FORMS_DIR)
        targets = sorted(set(DEFAULT_TRACK_CODES) | known)
        logging.info("Not enough discovered PDFs; probing %d known track codes…", len(targets))
        brute_saved = try_bruteforce_patterns(targets)
        wrote.update(brute_saved)

    # 3) Report
    if wrote:
        logging.info("Saved %d form PDFs:", len(wrote))
        for k, v in sorted(wrote.items()):
            logging.info("  %s -> %s", k, os.path.relpath(v))
    else:
        logging.warning("No PDFs saved. This can happen if forms are not yet published today.")
        # Touch a marker so downstream steps can branch
        marker = os.path.join(FORMS_DIR, f"EMPTY_{YMD}.txt")
        with open(marker, "w") as f:
            f.write("No PDFs available at fetch time.\n")

    # Exit 0 even if empty — downstream steps decide how to handle empty day
    return 0


if __name__ == "__main__":
    sys.exit(main())

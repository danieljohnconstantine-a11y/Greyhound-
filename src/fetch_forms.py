#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_forms.py — robust Racing & Sports PDF fetcher (AU greyhounds)

What it does
------------
1) Tries to DISCOVER today's greyhound form PDFs by scanning Racing & Sports
   greyhound listing pages for direct links to the CDN:
     https://files.racingandsports.com/racing/raceinfo/newformpdf/...
2) If discovery is thin/empty, it FALLS BACK to probing a few known filename
   patterns for a curated set of common AU track codes.
3) Saves any found PDFs to: ./forms/{TRACK}_{YYYY-MM-DD}.pdf
4) Writes a small manifest JSON at ./data/rns/forms_fetch_<UTC>.json
5) Always exits 0 so your workflow can continue even if nothing is live yet.

No Playwright/Chromium required. Only requests + bs4.
"""

from __future__ import annotations
import os
import re
import sys
import json
import time
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ------------------ Config ------------------

LISTING_URLS = [
    # Try both; one of these usually lists today's AU greyhound meetings
    "https://www.racingandsports.com.au/form-guide/greyhound/australia",
    "https://www.racingandsports.com.au/form-guide/greyhound",
]

CDN_HOST_SUBSTR = "files.racingandsports.com/racing/raceinfo/newformpdf"

# A practical set of AU track codes often seen in filenames (safe to probe)
DEFAULT_TRACK_CODES = [
    # NSW
    "WENT", "RICH", "DAPT", "GOSF", "MAIT", "BATH", "GRAF", "TARE", "GUNN",
    # VIC
    "MEAD", "SAND", "THEM", "BALL", "BEND", "WARN", "GEEL", "HORS", "SHEP", "HEAL",
    # QLD
    "ALBN", "IPSW", "BRIS", "ROCK", "TOWN", "CAPA",
    # SA
    "ANPK", "GAWL",
    # WA / NT / TAS
    "CANN", "MAND", "DRWN", "LAUN", "HOBT",
    # Common alternates seen historically (kept for coverage)
    "CANNG", "WENTG", "RICHG", "DAPTG", "MEADG", "SANDG", "THEMG",
]

# Filename patterns we’ll probe per track on fallback
PATTERNS = [
    # TRACK + DDMM + form.pdf (most common)
    "{track}{ddmm}form.pdf",
    # TRACK_ + DDMM + form.pdf (underscore variant)
    "{track}_{ddmm}form.pdf",
    # TRACK + DDMMYY + form.pdf (occasionally seen)
    "{track}{ddmmyy}form.pdf",
    # TRACK- + DDMM + form.pdf (rare)
    "{track}-{ddmm}form.pdf",
]

CDN_BASE = "https://files.racingandsports.com/racing/raceinfo/newformpdf"

HEADERS_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.racingandsports.com.au/",
}

HEADERS_PDF = {
    **HEADERS_HTML,
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
}

AUS_TZ = timezone(timedelta(hours=10))  # AEST baseline (good enough for daily naming)
TODAY = (datetime.utcnow() + timedelta(hours=10)).date()
YMD = TODAY.strftime("%Y-%m-%d")
DDMM = TODAY.strftime("%d%m")
DDMMYY = TODAY.strftime("%d%m%y")

FORMS_DIR = os.path.join("forms")
MANIFEST_DIR = os.path.join("data", "rns")

# ------------------ Helpers ------------------

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def utcstamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def http_get(url: str, headers: dict, timeout: int = 25) -> requests.Response:
    last_exc = None
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code in (200, 206):
                return r
            # soft backoff for 403/404/5xx
            time.sleep(random.uniform(0.6, 1.2) * attempt)
        except requests.RequestException as e:
            last_exc = e
            time.sleep(random.uniform(0.8, 1.6) * attempt)
    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"GET failed after retries: {url}")

def http_head_ok(url: str, headers: dict, timeout: int = 20) -> bool:
    try:
        r = requests.head(url, headers=headers, allow_redirects=True, timeout=timeout)
        if r.status_code == 200:
            ct = (r.headers.get("Content-Type") or "").lower()
            return ("pdf" in ct) or (ct == "" and int(r.headers.get("Content-Length", "0")) > 2000)
        if r.status_code in (301, 302, 303, 307, 308):
            g = requests.get(url, headers=headers, stream=True, timeout=timeout+5)
            ok = g.status_code == 200 and "pdf" in (g.headers.get("Content-Type","").lower())
            g.close()
            return ok
        return False
    except requests.RequestException:
        return False

def save_pdf(url: str, track: str) -> Optional[str]:
    name = f"{track}_{YMD}.pdf"
    out_path = os.path.join(FORMS_DIR, name)
    if os.path.exists(out_path) and os.path.getsize(out_path) > 4096:
        print(f"[skip] already have {name}")
        return out_path
    try:
        r = http_get(url, HEADERS_PDF, timeout=60)
        if r.status_code != 200:
            print(f"[miss] {track}: status {r.status_code} for {url}")
            return None
        # tiny guard against HTML error pages
        if len(r.content) < 4096 or b"%PDF" not in r.content[:1024]:
            print(f"[warn] {track}: tiny/invalid PDF ({len(r.content)} bytes) for {url}")
            return None
        with open(out_path, "wb") as f:
            f.write(r.content)
        print(f"[ok]   {track}: saved {name} ({len(r.content)//1024} KB)")
        return out_path
    except Exception as e:
        print(f"[err]  {track}: download failed for {url}: {e}")
        return None

def discover_from_listing() -> Dict[str, str]:
    """
    Scan R&S listing pages for <a href> links that point to the 'newformpdf' CDN.
    Return {TRACK_CODE: absolute_pdf_url}
    """
    found: Dict[str, str] = {}
    for page in LISTING_URLS:
        try:
            resp = http_get(page, HEADERS_HTML)
        except Exception as e:
            print(f"[warn] listing fetch failed {page}: {e}")
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

            file = href.split("/")[-1]
            # Try to parse track from filename start (letters before date)
            # e.g. CANN0609form.pdf or RICH130925form.pdf
            m = re.match(r"([A-Za-z]{3,6})[_\-]?\d{4,6}form\.pdf$", file)
            track = (m.group(1).upper() if m else "UNK")
            found.setdefault(track, href)

    return found

def brute_patterns(track_codes: List[str]) -> Dict[str, str]:
    """
    Try several known filename patterns per track against the CDN.
    Only include URLs that HEAD-check as existing PDFs.
    """
    urls: Dict[str, str] = {}
    for code in track_codes:
        for pat in PATTERNS:
            fname = pat.format(track=code, ddmm=DDMM, ddmmyy=DDMMYY)
            url = f"{CDN_BASE}/{fname}"
            if http_head_ok(url, HEADERS_PDF):
                urls[code] = url
                break  # next track
    return urls

# ------------------ Main ------------------

def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Fetch AU greyhound form PDFs (R&S) for today")
    ap.add_argument("--out", default="forms", help="output folder (default: forms)")
    ap.add_argument("--min-pdfs", type=int, default=1, help="try brute fallback until at least this many PDFs")
    args = ap.parse_args()

    global FORMS_DIR
    FORMS_DIR = args.out
    ensure_dir(FORMS_DIR)
    ensure_dir(MANIFEST_DIR)

    fetched: Dict[str, str] = {}
    ts = utcstamp()

    # 1) Discovery from listing pages
    print(f"[info] AU date {YMD} | discovering CDN links…")
    discovered = discover_from_listing()
    print(f"[info] discovered {len(discovered)} candidate links")

    # Try to save discovered PDFs first
    for track, url in discovered.items():
        path = save_pdf(url, track)
        if path:
            fetched[track] = path

    # 2) Fallback: brute patterns if too few so far
    if len(fetched) < args.min_pdfs:
        # Combine defaults with any track codes we already saw (for more coverage)
        fallback_codes = sorted(set(DEFAULT_TRACK_CODES) | set(discovered.keys()))
        print(f"[info] fallback probe on {len(fallback_codes)} track codes…")
        probed = brute_patterns(fallback_codes)
        for track, url in probed.items():
            if track in fetched:
                continue
            path = save_pdf(url, track)
            if path:
                fetched[track] = path

    # 3) Manifest
    manifest = {
        "fetched_at_utc": ts,
        "date_au": YMD,
        "count": len(fetched),
        "files": [{"track": k, "path": v} for k, v in sorted(fetched.items())],
    }
    man_path = os.path.join(MANIFEST_DIR, f"forms_fetch_{ts}.json")
    with open(man_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"[done] PDFs saved: {len(fetched)} | manifest: {man_path}")

    # Always succeed (downstream handles empty day gracefully)
    return 0


if __name__ == "__main__":
    sys.exit(main())

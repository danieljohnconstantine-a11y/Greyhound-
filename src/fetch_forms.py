#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch today's AU greyhound form PDFs from Racing & Sports.

1) Discover CDN PDF links from R&S listing pages.
2) If thin, brute-probe multiple filename patterns for common track codes.
3) Save PDFs => ./forms/{TRACK}_{YYYY-MM-DD}.pdf
4) Write manifest => ./data/rns/forms_fetch_<UTC>.json
5) Always exit 0 (downstream handles empty days).
"""

from __future__ import annotations
import os, re, sys, json, time, random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

LISTING_URLS = [
    "https://www.racingandsports.com.au/form-guide/greyhound/australia",
    "https://www.racingandsports.com.au/form-guide/greyhound",
]

CDN_BASE = "https://files.racingandsports.com/racing/raceinfo/newformpdf"

DEFAULT_TRACK_CODES = [
    # NSW
    "WENT","RICH","DAPT","GOSF","MAIT","BATH","GRAF","TARE","GUNN","WAGG",
    # VIC
    "MEAD","SAND","THEM","BALL","BEND","WARN","GEEL","HORS","SHEP","HEAL","TRAR","SALE",
    # QLD
    "ALBN","IPSW","BRIS","ROCK","TOWN","CAPA",
    # SA
    "ANPK","GAWL","MURR",
    # WA / NT / TAS
    "CANN","MAND","DRWN","LAUN","HOBT",
    # Common alternates with 'G' suffix
    "WENTG","RICHG","DAPTG","MEADG","SANDG","THEMG","CANNG",
]

PATTERNS = [
    "{track}{ddmm}form.pdf",
    "{track}_{ddmm}form.pdf",
    "{track}{ddmmyy}form.pdf",
    "{track}-{ddmm}form.pdf",
]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS_HTML = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.racingandsports.com.au/",
}
HEADERS_PDF = {
    **HEADERS_HTML,
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
}

AUS_TZ = timezone(timedelta(hours=10))
TODAY = (datetime.utcnow() + timedelta(hours=10)).date()
YMD = TODAY.strftime("%Y-%m-%d")
DDMM = TODAY.strftime("%d%m")
DDMMYY = TODAY.strftime("%d%m%y")

FORMS_DIR = "forms"
MANIFEST_DIR = os.path.join("data", "rns")

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
            time.sleep(random.uniform(0.5, 1.2) * attempt)
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
            if "pdf" in ct:
                return True
            # Sometimes PDF served with empty ct but nonzero size
            try:
                size = int(r.headers.get("Content-Length", "0"))
                return size > 2000
            except Exception:
                return False
        if r.status_code in (301,302,303,307,308):
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
            print(f"[miss] {track}: status {r.status_code} {url}")
            return None
        if len(r.content) < 4096 or b"%PDF" not in r.content[:1024]:
            print(f"[warn] {track}: tiny/invalid PDF ({len(r.content)} B) {url}")
            return None
        with open(out_path, "wb") as f:
            f.write(r.content)
        print(f"[ok]   {track}: {name} ({len(r.content)//1024} KB)")
        return out_path
    except Exception as e:
        print(f"[err]  {track}: download failed {url}: {e}")
        return None

def discover_from_listing() -> Dict[str, str]:
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
            if "newformpdf" not in href or not href.endswith(".pdf"):
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.racingandsports.com.au" + href
            file = href.split("/")[-1]
            m = re.match(r"([A-Za-z]{3,6})[_\-]?\d{4,6}form\.pdf$", file)
            track = (m.group(1).upper() if m else "UNK")
            found.setdefault(track, href)
    return found

def brute_patterns(track_codes: List[str]) -> Dict[str, str]:
    urls: Dict[str, str] = {}
    for code in track_codes:
        for pat in PATTERNS:
            fname = pat.format(track=code, ddmm=DDMM, ddmmyy=DDMMYY)
            url = f"{CDN_BASE}/{fname}"
            if http_head_ok(url, HEADERS_PDF):
                urls[code] = url
                break
    return urls

def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Fetch AU greyhound form PDFs (R&S)")
    ap.add_argument("--out", default="forms")
    ap.add_argument("--min-pdfs", type=int, default=1)
    args = ap.parse_args()

    global FORMS_DIR
    FORMS_DIR = args.out
    ensure_dir(FORMS_DIR)
    ensure_dir(MANIFEST_DIR)

    ts = utcstamp()
    fetched: Dict[str, str] = {}

    print(f"[info] AU date {YMD} — discovering CDN links…")
    discovered = discover_from_listing()
    print(f"[info] discovered {len(discovered)} candidates")

    for track, url in discovered.items():
        path = save_pdf(url, track)
        if path:
            fetched[track] = path

    if len(fetched) < args.min_pdfs:
        fallbacks = sorted(set(DEFAULT_TRACK_CODES) | set(discovered.keys()))
        print(f"[info] fallback probing {len(fallbacks)} track codes…")
        probed = brute_patterns(fallbacks)
        for track, url in probed.items():
            if track in fetched:
                continue
            path = save_pdf(url, track)
            if path:
                fetched[track] = path

    manifest = {
        "fetched_at_utc": ts,
        "date_au": YMD,
        "count": len(fetched),
        "files": [{"track": k, "path": v} for k, v in sorted(fetched.items())],
    }
    man_path = os.path.join(MANIFEST_DIR, f"forms_fetch_{ts}.json")
    with open(man_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"[done] PDFs saved: {len(fetched)} → {man_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

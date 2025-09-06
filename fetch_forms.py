#!/usr/bin/env python3
from __future__ import annotations
import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup

BASE_RAS = "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf"

# Known RAS PDF codes per track (extend as you learn more codes)
TRACK_CODES = {
    "RICH": ["RICHG"],   # Richmond
    "HEAL": ["HEALG"],   # Healesville
    "GAWL": ["GAWLG"],   # Gawler
    "QSTR": ["QSTRG"],   # Qld Straight (example)
    "GRAF": ["GRAFG"],   # Grafton
    "DRWN": ["DRWNG"],   # Darwin
    "CANN": ["CANNG"],   # Cannington
    # "WENT": ["WENTG"], # Wentworth Park (add when confirmed)
    # "BEND": ["BENDG"], # Bendigo (add when confirmed)
}

INDEX_PAGES = [
    "https://www.racingandsports.com.au/form-guide/greyhound",
    "https://www.thegreyhoundrecorder.com.au/form-guides/",
]

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/123.0 Safari/537.36")
PDF_LINK_PAT = re.compile(r"form\.pdf$", re.IGNORECASE)

def resolve_date(s: str) -> dt.date:
    if s.lower() == "today":
        return dt.date.today()
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def http_get(url: str, timeout: float = 25.0):
    try:
        return requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    except requests.RequestException:
        return None

def try_direct_pdf(code: str, d: dt.date, timeout: float) -> Tuple[str, bytes] | None:
    dd = f"{d.day:02d}"
    mm = f"{d.month:02d}"
    url = f"{BASE_RAS}/{code}{dd}{mm}form.pdf"
    r = http_get(url, timeout)
    if r and r.status_code == 200 and r.headers.get("content-type","").lower().startswith("application/pdf"):
        return url, r.content
    return None

def harvest_pdf_links(index_url: str, timeout: float) -> List[str]:
    links: List[str] = []
    r = http_get(index_url, timeout)
    if not r or r.status_code != 200:
        return links
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.select("a[href]"):
        href = a["href"].strip()
        if PDF_LINK_PAT.search(href):
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(index_url, href)
            links.append(href)
    # de-dup keep order
    seen = set(); out=[]
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def infer_track_from_filename(url_or_name: str) -> str | None:
    base = url_or_name.split("/")[-1]
    m = re.search(r"([A-Za-z]+)0?\d{2}0?\d{2}form\.pdf$", base, re.IGNORECASE)
    if not m: return None
    raw = m.group(1).upper()
    if raw.endswith("G") and len(raw) > 1:  # strip greyhound suffix
        raw = raw[:-1]
    return raw[:5]

def save_pdf(dest_dir: Path, track: str, d: dt.date, content: bytes) -> Path:
    dest = dest_dir / f"{track}_{d.isoformat()}.pdf"
    with open(dest, "wb") as f:
        f.write(content)
    return dest

def main() -> int:
    ap = argparse.ArgumentParser("Fetch today's greyhound Long Form PDFs")
    ap.add_argument("--date", default="today", help="YYYY-MM-DD or 'today'")
    ap.add_argument("--out", default="forms", help="Output folder")
    ap.add_argument("--timeout", type=float, default=25.0)
    args = ap.parse_args()

    run_date = resolve_date(args.date)
    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)

    found_any = False

    # Plan A: direct RAS codes
    for short, codes in TRACK_CODES.items():
        for code in codes:
            got = try_direct_pdf(code, run_date, args.timeout)
            if not got: continue
            url, content = got
            p = save_pdf(outdir, short, run_date, content)
            print(f"[direct] {code} -> {p.name}")
            found_any = True
            break

    # Plan B: crawl hubs for any ...form.pdf links
    for idx in INDEX_PAGES:
        for url in harvest_pdf_links(idx, args.timeout):
            r = http_get(url, args.timeout)
            if not r or r.status_code != 200 or not r.headers.get("content-type","").lower().startswith("application/pdf"):
                continue
            track = infer_track_from_filename(url) or "TRACK"
            p = save_pdf(outdir, track, run_date, r.content)
            print(f"[crawl] {url} -> {p.name}")
            found_any = True

    if not found_any:
        print(f"No Long Form PDFs found for {run_date}. (OK if not published yet)")
        return 0  # exit green

    for f in sorted(outdir.glob(f"*_{run_date.isoformat()}.pdf")):
        print("saved:", f.name)
    return 0

if __name__ == "__main__":
    sys.exit(main())

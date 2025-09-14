#!/usr/bin/env python3
import os
import re
import time
import json
import datetime as dt
from typing import List, Dict
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

# Only keep canonical R&S newform PDFs, named like QSTR_2025-09-08.pdf
PDF_HOST = "https://files.racingandsports.com"
PDF_PATH = "/racing/raceinfo/newformpdf/"

FNAME_RE = re.compile(r"^([A-Z]{4})_(\d{4}-\d{2}-\d{2})\.pdf$")

# Known AU greyhound track codes seen in repo forms
TRACK_CODES = [
    "QSTR","RICH","SALE","HEAL","GRAF","CANN","GAWL","CAPA","DRWN","CAPE","WENT","SAND","BEND","GEEL",
    "BALL","WARR","BATH","BULL","DAPT","TARE","MAIT","ALBU","GOSF","NOWR","ANGLE","HOBT","LAUN","DEVN",
    "ROCK","IPSW","ALBN","MAND","NTHM","SHEP","HORS","GOLD","BROK"  # include a few commons
]

def sydney_today() -> str:
    # local date in AEST/AEDT
    from dateutil import tz
    tz_syd = tz.gettz("Australia/Sydney")
    return dt.datetime.now(tz_syd).strftime("%Y-%m-%d")

def ddg_search(query: str) -> List[str]:
    """DuckDuckGo HTML search → list of result links."""
    url = "https://duckduckgo.com/html/"
    params = {"q": query}
    r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.select("a.result__a"):
        href = a.get("href", "")
        if href.startswith("http"):
            out.append(href)
    return out

def is_target_pdf(url: str, code: str, date_str: str) -> bool:
    if not url.startswith(PDF_HOST) or PDF_PATH not in url:
        return False
    fn = url.rsplit("/", 1)[-1]
    m = FNAME_RE.match(fn)
    if not m:
        return False
    c, d = m.group(1), m.group(2)
    return (c == code and d == date_str)

def download(url: str, out_dir: str) -> str | None:
    fn = url.rsplit("/", 1)[-1]
    path = os.path.join(out_dir, fn)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    with requests.get(url, headers={"User-Agent": UA, "Referer": "https://duckduckgo.com/"}, timeout=60, stream=True) as r:
        if r.status_code != 200 or "application/pdf" not in r.headers.get("content-type",""):
            return None
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)
    return path

def fetch_for_date(date_str: str, out_dir: str) -> Dict[str, List[str]]:
    os.makedirs(out_dir, exist_ok=True)
    found: Dict[str, List[str]] = {c: [] for c in TRACK_CODES}

    # Query per track; keep it simple and reliable
    for code in TRACK_CODES:
        # Example: site:files.racingandsports.com/racing/raceinfo/newformpdf filetype:pdf "QSTR_2025-09-08.pdf"
        quoted = f"\"{code}_{date_str}.pdf\""
        q = f"site:{PDF_HOST}{PDF_PATH} filetype:pdf {quoted}"
        links = ddg_search(q)
        # tiny backoff to be nice
        time.sleep(0.8)

        for link in links:
            if is_target_pdf(link, code, date_str):
                saved = download(link, out_dir)
                if saved:
                    found[code].append(saved)

    # Remove empty keys
    found = {k: v for k, v in found.items() if v}
    # manifest
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    with open(os.path.join(out_dir, f"manifest_{stamp}.json"), "w") as f:
        json.dump({"date": date_str, "files": found}, f, indent=2)
    return found

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=sydney_today())
    ap.add_argument("--out", default="forms")
    args = ap.parse_args()
    fetched = fetch_for_date(args.date, args.out)
    print(f"[fetch] date={args.date} total_files={sum(len(v) for v in fetched.values())}")

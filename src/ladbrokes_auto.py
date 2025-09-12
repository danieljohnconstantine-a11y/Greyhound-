"""
Scraper with fallback:
- Try Ladbrokes first (may 403).
- If Ladbrokes fails or returns no meetings, try TAB (tab.com.au).
- Always write JSON + debug HTML into data/ladbrokes/.
"""

from __future__ import annotations
import os, sys, json, time, argparse
from datetime import datetime, timezone
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Targets
LADBROKES_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"
TAB_URL = "https://www.tab.com.au/racing/meetings/greyhound"

# Browser-like headers
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
}

def session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, backoff_factor=0.8,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def fetch_html(sess: requests.Session, url: str) -> tuple[int, str]:
    try:
        r = sess.get(url, timeout=25)
        return r.status_code, r.text or ""
    except Exception as e:
        return -1, f"__EXC__:{e}"

def parse_ladbrokes(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    meetings: List[Dict] = []
    for a in soup.select('a[href*="/racing/greyhound"]'):
        href = a.get("href") or ""
        name = a.get_text(strip=True)
        if not href or not name:
            continue
        if href.startswith("/"):
            href = "https://www.ladbrokes.com.au" + href
        if "results" in href.lower():
            continue
        meetings.append({"name": name, "url": href})
    return meetings

def parse_tab(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    meetings: List[Dict] = []
    for a in soup.select("a[href*='/racing/']"):
        href = a.get("href") or ""
        name = a.get_text(strip=True)
        if not href or not name:
            continue
        if href.startswith("/"):
            href = "https://www.tab.com.au" + href
        meetings.append({"name": name, "url": href})
    return meetings

def main(out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    stamp = utc_stamp()
    out_json = os.path.join(out_dir, f"meetings_{stamp}.json")
    out_debug = os.path.join(out_dir, f"debug_{stamp}.html")

    sess = session()

    # 1) Try Ladbrokes
    status, html = fetch_html(sess, LADBROKES_URL)
    meetings = parse_ladbrokes(html) if status == 200 else []
    source = "ladbrokes"

    # 2) Fallback → TAB
    if not meetings:
        status, html = fetch_html(sess, TAB_URL)
        meetings = parse_tab(html) if status == 200 else []
        source = "tab"

    # Write debug HTML always
    with open(out_debug, "w", encoding="utf-8", errors="ignore") as f:
        f.write(html or f"<!-- empty response status={status} -->")

    # Write JSON summary
    payload = {
        "fetched_at_utc": stamp,
        "source": source,
        "status_code": status,
        "count": len(meetings),
        "meetings": meetings,
    }
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"source={source} status={status} meetings={len(meetings)}")
    print(f"wrote: {out_json}")
    print(f"saved debug: {out_debug}")

    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes")
    args = ap.parse_args()
    sys.exit(main(args.out_dir))

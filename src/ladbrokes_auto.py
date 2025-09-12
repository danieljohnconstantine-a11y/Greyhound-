"""
Auto-discover Ladbrokes greyhound meetings and save:
- data/ladbrokes/ladbrokes_meetings_<UTC>.json  (parsed links & names)
- data/ladbrokes/debug_<UTC>.html               (raw page for inspection)

This script **never exits non-zero**; even on 403 it writes an empty JSON
and a debug HTML so the workflow can commit artifacts for diagnosis.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


GREYHOUNDS_URLS = [
    # Try desktop first, then a couple of known alternates
    "https://www.ladbrokes.com.au/racing/greyhound-racing",
    "https://www.ladbrokes.com.au/racing/greyhounds",
    "https://www.ladbrokes.com.au/",
]

# A realistic browser header set
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}


def _session() -> requests.Session:
    s = requests.Session()
    # Robust retry policy, including 403/429 which we sometimes see from WAF/CDN
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=(403, 408, 425, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(BROWSER_HEADERS)
    return s


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fetch_any(session: requests.Session, urls: List[str]) -> requests.Response:
    """
    Try a small sequence of URLs; do a warmup hit to the site root to set cookies;
    add a cache-busting `_` query to look less bot-like.
    """
    # Warmup/home to set cookies
    try:
        session.get("https://www.ladbrokes.com.au/", timeout=15)
    except Exception:
        pass

    last_resp = None
    for base in urls:
        try:
            sep = "&" if "?" in base else "?"
            url = f"{base}{sep}_={int(time.time()*1000)}"
            last_resp = session.get(url, timeout=20)
            # 200-299 → good; 403/429 etc will be retried by urllib3 policy already
            if 200 <= last_resp.status_code < 300 and last_resp.text:
                return last_resp
        except Exception:
            continue
    # Return the last response (possibly 403) so we can dump HTML for debugging
    return last_resp


def parse_meetings(html: str) -> List[Dict]:
    """
    Very liberal parser: find anchors that look like greyhound meeting links.
    We keep it heuristic so minor markup changes don't break us.
    """
    soup = BeautifulSoup(html, "lxml")
    meetings: List[Dict] = []
    seen = set()

    # Common patterns we might see
    selectors = [
        'a[href*="/racing/greyhound"]',
        'a[href*="/greyhound-racing"]',
        'a[href*="/greyhound/"]',
    ]

    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href") or ""
            name = a.get_text(strip=True) or ""
            if not href:
                continue
            # Normalise href → absolute
            if href.startswith("/"):
                href = "https://www.ladbrokes.com.au" + href
            key = (href, name)
            if key in seen:
                continue
            # Basic quality filters
            if "results" in href.lower() or "replays" in href.lower():
                continue
            if not name or len(name) < 3:
                continue
            seen.add(key)
            meetings.append({"name": name, "url": href})

    return meetings


def main(out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    stamp = _utc_stamp()
    out_json = os.path.join(out_dir, f"ladbrokes_meetings_{stamp}.json")
    out_debug = os.path.join(out_dir, f"debug_{stamp}.html")

    sess = _session()
    resp = fetch_any(sess, GREYHOUNDS_URLS)

    status = resp.status_code if resp is not None else -1
    text = resp.text if (resp is not None and resp.text) else ""

    # Always save the debug HTML we got (even if 403/empty)
    try:
        with open(out_debug, "w", encoding="utf-8") as f:
            f.write(text or f"<!-- Empty body; status={status} -->")
    except Exception as e:
        print(f"warn: could not write debug HTML: {e}", file=sys.stderr)

    meetings: List[Dict] = []
    if 200 <= status < 300 and text:
        try:
            meetings = parse_meetings(text)
        except Exception as e:
            print(f"warn: parse failed: {e}", file=sys.stderr)

    # Save JSON (maybe empty list; that’s OK)
    try:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "fetched_at_utc": stamp,
                    "status_code": status,
                    "count": len(meetings),
                    "meetings": meetings,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
    except Exception as e:
        print(f"warn: could not write meetings JSON: {e}", file=sys.stderr)

    print(f"status={status} meetings={len(meetings)}")
    print(f"wrote: {out_json}")
    print(f"saved debug: {out_debug}")

    # Always exit 0 so the workflow continues to commit/upload artifacts
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="data/ladbrokes")
    args = p.parse_args()
    sys.exit(main(args.out_dir))

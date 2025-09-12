# src/ladbrokes_auto.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import time
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LADBROKES_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"

# A very complete header set to look like a real Chrome on Windows.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.ladbrokes.com.au/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    # These sec-* headers are commonly sent by Chrome; help some WAFs
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


def session_with_retries() -> requests.Session:
    """Return a requests Session with robust retries for 403/429/5xx."""
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=[403, 408, 409, 425, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def scrape_ladbrokes_meetings() -> List[Dict]:
    """
    Download the greyhound racing page and extract meeting links/names.

    Returns:
        A list of {name, url} dictionaries (may be empty if blocked).
    """
    s = session_with_retries()

    # Small jitter can help with anti-bot (no harm if not needed)
    time.sleep(0.7)

    resp = s.get(LADBROKES_URL, timeout=20)
    # Don't raise immediately: 403/429 may succeed on retry with backoff
    if resp.status_code == 403:
        # One extra deliberate wait then a final attempt
        time.sleep(2.0)
        resp = s.get(LADBROKES_URL, timeout=20)

    # If still not OK, return with as much debug as is safe
    if resp.status_code >= 400:
        print(
            f"[warn] Got HTTP {resp.status_code} from Ladbrokes. "
            f"Anti-bot may be blocking datacenter IPs."
        )
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    meetings: List[Dict] = []

    # Heuristics: find anchors that look like meeting links.
    # Adjust patterns as we learn the DOM (kept conservative and safe).
    link_candidates = soup.select("a[href]")

    pattern = re.compile(r"/racing/greyhound-racing/[^\"']+", re.IGNORECASE)

    for a in link_candidates:
        href = a.get("href", "")
        if not href:
            continue
        if not pattern.search(href):
            continue

        # Normalize URL
        url = href
        if url.startswith("/"):
            url = f"https://www.ladbrokes.com.au{url}"

        # Visible text as name
        name = a.get_text(strip=True) or "Unknown"

        # Filter obvious non-meeting junk (very soft filter)
        if len(name) < 3:
            continue

        meetings.append({"name": name, "url": url})

    # De-duplicate by URL while preserving order
    seen = set()
    uniq: List[Dict] = []
    for m in meetings:
        if m["url"] in seen:
            continue
        seen.add(m["url"])
        uniq.append(m)

    print(f"[info] Extracted {len(uniq)} candidate meeting links.")
    return uniq


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Ladbrokes greyhound meetings.")
    parser.add_argument(
        "--out-dir",
        default="data/ladbrokes",
        help="Directory to write JSON outputs into (default: data/ladbrokes)",
    )
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    meetings = scrape_ladbrokes_meetings()

    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    out_file = os.path.join(args.out_dir, f"meetings_{today}.json")

    payload = {
        "source": "ladbrokes",
        "scraped_at_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "page": LADBROKES_URL,
        "count": len(meetings),
        "meetings": meetings,
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"[ok] Saved {len(meetings)} meetings to {out_file}")


if __name__ == "__main__":
    main()

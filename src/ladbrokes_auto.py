# src/ladbrokes_auto.py
import os
import json
import time
import argparse
from datetime import datetime, timezone
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry


LADBROKES_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"

# Strong, browser-like headers
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
              "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.ladbrokes.com.au/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _session_with_retries() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def fetch_greyhound_page() -> requests.Response:
    """
    Fetch the greyhound landing page using strong headers and optional cookie.
    If you set a repo secret called LADBROKES_COOKIE, it will be sent with the request.
    """
    headers = dict(BASE_HEADERS)

    # Optional: send cookie captured from a manual browser visit
    cookie = os.getenv("LADBROKES_COOKIE", "").strip()
    cookies = {}
    if cookie:
        # Accept either a raw "name=value; name2=value2" string or JSON {"name":"value",...}
        if "=" in cookie and ("{" not in cookie):
            headers["Cookie"] = cookie
        else:
            # JSON style
            try:
                import json as _json
                parsed = _json.loads(cookie)
                if isinstance(parsed, dict):
                    cookies = parsed
            except Exception:
                headers["Cookie"] = cookie  # fallback as raw

    sess = _session_with_retries()
    resp = sess.get(LADBROKES_URL, headers=headers, cookies=cookies, timeout=30)
    return resp


def parse_meetings(html: str) -> List[Dict]:
    """
    Very lenient parser: look for anchors that smell like meetings.
    This is intentionally broad; we just want *some* meeting info to prove we’re
    past the interstitial/blocked page.
    """
    soup = BeautifulSoup(html, "lxml")

    # If page has obvious block markers, return empty
    text = soup.get_text(" ", strip=True).lower()
    blocked_keywords = [
        "access denied", "forbidden", "restricted", "verify you are human",
        "bot", "not authorized", "please enable javascript"
    ]
    if any(k in text for k in blocked_keywords):
        return []

    meetings: List[Dict] = []

    # Heuristics: anchors or divs that contain meeting names/links
    # Try anchors under greyhound context
    for a in soup.find_all("a", href=True):
        label = a.get_text(strip=True)
        href = a["href"]
        if not label:
            continue
        # meeting-like hints
        lc = label.lower()
        if any(x in lc for x in ["race ", "racing", "greyhound", "dogs", "meeting", "park", "track"]):
            # absolutize link if needed
            if href.startswith("/"):
                href = "https://www.ladbrokes.com.au" + href
            meetings.append({"name": label, "url": href})

    # Deduplicate by URL
    seen = set()
    deduped = []
    for m in meetings:
        if m["url"] in seen:
            continue
        seen.add(m["url"])
        deduped.append(m)

    return deduped


def save_debug(out_dir: str, html: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    debug_path = os.path.join(out_dir, f"debug_{ts}.html")
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)
    return debug_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes", help="Output folder")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    resp = fetch_greyhound_page()
    status = resp.status_code
    html = resp.text

    # Always save debug HTML (helps diagnose when blocked)
    debug_path = save_debug(args.out_dir, html)

    meetings = []
    if 200 <= status < 300 and html:
        meetings = parse_meetings(html)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_json = os.path.join(args.out_dir, f"meetings_{ts}.json")
    payload = {
        "fetched_at_utc": ts,
        "source": "ladbrokes_tab",
        "status_code": status,
        "count": len(meetings),
        "meetings": meetings,
    }
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"status={status} meetings={len(meetings)}")
    print(f"wrote: {out_json}")
    print(f"saved debug: {debug_path}")

    # If still blocked, exit 0 so the workflow can continue to upload artifacts/commit
    if status in (403, 429, 500) and len(meetings) == 0:
        # small sleep to stagger repeated attempts
        time.sleep(1)
        return 0


if __name__ == "__main__":
    main()

# src/ladbrokes_auto.py
from __future__ import annotations
import os, sys, json, time, random, argparse, datetime as dt
from typing import Tuple, List, Dict
import requests
from bs4 import BeautifulSoup

BASE = "https://www.ladbrokes.com.au"
LIST_URL = f"{BASE}/racing/greyhound-racing"

# desktop-ish headers + keepalive; helps reduce easy 403s
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

def fetch_with_retries(url: str, tries: int = 4, backoff: float = 0.8) -> Tuple[int, str]:
    s = requests.Session()
    s.headers.update(HEADERS)
    status = -1
    text = ""
    for attempt in range(1, tries + 1):
        try:
            r = s.get(url, timeout=25)
            status, text = r.status_code, (r.text or "")
            if status == 200 and text.strip():
                return status, text
            # 403/5xx: brief backoff + retry
        except Exception as e:
            status, text = -1, f"__EXC__:{e!r}"
        time.sleep(backoff * attempt + random.uniform(0, 0.4))
    return status, text

def parse_meetings(html: str) -> List[Dict[str,str]]:
    soup = BeautifulSoup(html, "lxml")
    results: List[Dict[str, str]] = []
    seen = set()

    # generic “greyhound” links
    for a in soup.select('a[href*="/racing/greyhound"]'):
        name = (a.get_text(strip=True) or "").strip()
        href = a.get("href") or ""
        if not href:
            continue
        if not href.startswith("http"):
            href = BASE + href
        key = (name.lower(), href.lower())
        # exclude category root pages
        if "greyhound-racing" in href and key not in seen and name:
            seen.add(key)
            results.append({"name": name, "href": href})

    return results

def ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes")
    args = ap.parse_args()
    ensure_dir(args.out_dir)

    status, html = fetch_with_retries(LIST_URL)

    payload = {
        "source": LIST_URL,
        "fetched_at": dt.datetime.utcnow().isoformat() + "Z",
        "status": status,
        "meetings": [],
    }

    if status == 200 and html and not html.startswith("__EXC__"):
        payload["meetings"] = parse_meetings(html)
    else:
        payload["debug_html"] = html[:200000]

    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    json_path = os.path.join(args.out_dir, f"ladbrokes_meetings_{stamp}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"status={status} meetings={len(payload['meetings'])}")
    print(f"wrote: {json_path}")

    if status != 200 or not payload["meetings"]:
        # also dump HTML to inspect exactly what the runner saw
        dbg_path = os.path.join(args.out_dir, f"debug_{stamp}.html")
        with open(dbg_path, "w", encoding="utf-8", errors="ignore") as f:
            f.write(payload.get("debug_html", html or "(no html)"))
        print(f"saved debug: {dbg_path}")
        sys.exit(2)  # fail so we notice it

if __name__ == "__main__":
    main()

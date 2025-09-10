import os
import sys
import json
import time
import requests
from bs4 import BeautifulSoup


URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.ladbrokes.com.au/",
    "Connection": "keep-alive",
}


def fetch_html(url: str, retries: int = 3, delay: float = 2.0) -> str:
    """Fetch page HTML with retries."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            print(f"[ladbrokes] GET {url} -> {r.status_code}")
            if r.status_code == 200:
                return r.text
            elif r.status_code in (403, 429, 503):
                time.sleep(delay * attempt)
                continue
            else:
                r.raise_for_status()
        except Exception as e:
            print(f"[ladbrokes] error {e} (attempt {attempt})")
            time.sleep(delay * attempt)
    sys.exit(1)


def parse_meetings(html: str):
    """Extract greyhound meeting links from Ladbrokes HTML."""
    soup = BeautifulSoup(html, "lxml")
    meetings = []
    seen = set()

    for a in soup.select("a[href*='/racing/greyhound']"):
        href = (a.get("href") or "").strip()
        name = a.get_text(" ", strip=True)
        if not href or not name:
            continue
        if not href.startswith("http"):
            href = "https://www.ladbrokes.com.au" + href
        if href in seen:
            continue
        seen.add(href)
        meetings.append({"name": name, "url": href})

    return meetings


def main(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    html = fetch_html(URL)
    meetings = parse_meetings(html)

    out_path = os.path.join(out_dir, "ladbrokes_meetings.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {"count": len(meetings), "meetings": meetings},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"[ladbrokes] ✅ Saved {len(meetings)} meetings -> {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data/ladbrokes", help="Output folder")
    args = parser.parse_args()
    main(args.out_dir)

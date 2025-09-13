#!/usr/bin/env python3
import argparse, json, os, re, sys, time
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

BASE = "https://www.racingandsports.com.au/form-guide/greyhound"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

def utcstamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get(url: str) -> tuple[int, str]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Referer": "https://www.racingandsports.com.au/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
    }
    r = requests.get(url, headers=headers, timeout=30)
    # Don't raise — we want to write debug even on 403/500
    return r.status_code, r.text

def parse_meetings(html: str):
    soup = BeautifulSoup(html, "lxml")
    meetings = []
    # Try a few generic patterns so we don't 500 if markup shifts
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if not text:
            continue
        # Heuristic: form-guide/greyhound/<track> in URL
        if "/form-guide/greyhound" in href and len(text) >= 3:
            url = href if href.startswith("http") else f"https://www.racingandsports.com.au{href}"
            meetings.append({"name": text, "url": url})
    # De-dupe by URL
    seen, uniq = set(), []
    for m in meetings:
        if m["url"] in seen: 
            continue
        seen.add(m["url"]); uniq.append(m)
    return uniq

def main(out_dir: str) -> int:
    ts = utcstamp()
    os.makedirs(out_dir, exist_ok=True)

    status, html = get(BASE)

    # Save debug HTML always
    debug_path = os.path.join(out_dir, f"debug_{ts}.html")
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)

    meetings = []
    try:
        if 200 <= status < 300:
            meetings = parse_meetings(html)
    except Exception as e:
        # Keep going; we still write JSON with 0 meetings
        pass

    out_json = {
        "fetched_at_utc": ts,
        "source": "rns",
        "status_code": status,
        "count": len(meetings),
        "meetings": meetings,
    }
    json_path = os.path.join(out_dir, f"meetings_{ts}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    print(f"status={status} meetings={len(meetings)}")
    # ✅ Always exit 0 so workflow continues, even if blocked
    return 0

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()
    sys.exit(main(args.out_dir))

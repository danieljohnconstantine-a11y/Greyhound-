from __future__ import annotations
import json, re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict
import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.ladbrokes.com.au"
GREYHOUNDS_HOME = f"{BASE}/racing/greyhound-racing"

OUT_DIR = Path("data")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
CSV_PATH = OUT_DIR / f"ladbrokes_{TODAY}.csv"
MEETINGS_JSON = OUT_DIR / f"ladbrokes_meetings_{TODAY}.json"
LOG_PATH = OUT_DIR / f"ladbrokes_log_{TODAY}.txt"

HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}

def log(msg: str) -> None:
    print(msg, flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f: f.write(msg + "\n")

def get_todays_meetings() -> List[str]:
    log(f"[info] Fetching meetings list: {GREYHOUNDS_HOME}")
    r = requests.get(GREYHOUNDS_HOME, headers=HEADERS, timeout=30); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/racing/greyhound-racing/" in href and ("race" in href or "meeting" in href):
            links.add(href)
    meetings = []
    for href in links:
        meetings.append(href if href.startswith("http") else BASE + href)
    log(f"[info] Discovered {len(meetings)} candidate meeting URLs")
    return sorted(set(meetings))

def scrape_meeting(meeting_url: str) -> List[Dict]:
    rows: List[Dict] = []
    log(f"[info] Scraping meeting: {meeting_url}")
    r = requests.get(meeting_url, headers=HEADERS, timeout=30); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    # Heuristics to capture "Race N" with optional name
    for tag in soup.find_all(["a","li","div","article"]):
        text = tag.get_text(" ", strip=True)
        m = re.search(r"\brace\s*(\d+)\b", text, re.I)
        if not m: continue
        race_no = int(m.group(1))
        m2 = re.search(r"\brace\s*\d+\s*[-:]\s*(.+)", text, re.I)
        race_name = (m2.group(1).strip() if m2 else text)[:200]
        key = (race_no, race_name)
        if key in seen: continue
        rows.append({"date_utc": TODAY, "meeting_url": meeting_url, "race_no": race_no, "race_name": race_name})
        seen.add(key)
    log(f"[info] Extracted {len(rows)} races")
    return rows

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meetings = get_todays_meetings()
    MEETINGS_JSON.write_text(json.dumps(meetings, indent=2), encoding="utf-8")
    all_rows: List[Dict] = []
    for murl in meetings:
        try: all_rows.extend(scrape_meeting(murl))
        except Exception as e: log(f"[error] {murl}: {e}")
    pd.DataFrame(all_rows, columns=["date_utc","meeting_url","race_no","race_name"])\
      .sort_values(["meeting_url","race_no"]).to_csv(CSV_PATH, index=False)
    log(f"[info] Wrote CSV: {CSV_PATH}")

if __name__ == "__main__":
    main()

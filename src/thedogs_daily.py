# src/thedogs_daily.py
# The Dogs daily AU greyhound fields/odds/results (lightweight)
# Outputs:
#   data/thedogs/meetings_<TS>.json
#   data/thedogs/full_day_<TS>.json
#   data/thedogs/full_day_<TS>.csv

from __future__ import annotations
import os, re, csv, json, time
from datetime import datetime, timezone
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup

BASE = "https://www.thedogs.com.au"
RACECARDS = f"{BASE}/racing/racecards"

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Referer": BASE + "/",
    }, timeout=timeout)
    r.raise_for_status()
    return r.text

def absol(href: str) -> str:
    if href.startswith("http"): return href
    if href.startswith("/"): return BASE + href
    return href

def scrape_meetings() -> List[Dict[str, str]]:
    html = get(RACECARDS)
    soup = BeautifulSoup(html, "lxml")
    meetings = []
    for a in soup.select("a[href*='/racing/'][href*='/20']"):  # often yyyy-mm-dd path
        name = (a.get_text(" ", strip=True) or "").strip()
        href = a.get("href") or ""
        if not name or not href: continue
        url = absol(href)
        if "racing/" in url and re.search(r"/\d{4}-\d{2}-\d{2}/", url):
            meetings.append({"name": name, "url": url})
    # de-dupe
    seen, out = set(), []
    for m in meetings:
        if m["url"] in seen: continue
        seen.add(m["url"]); out.append(m)
    return out

def parse_meeting(url: str) -> Dict[str, Any]:
    html = get(url)
    soup = BeautifulSoup(html, "lxml")
    title = soup.select_one("h1, .event-title, .page-title")
    meeting_name = (title.get_text(" ", strip=True) if title else url)

    races = []
    # race cards / items
    blocks = soup.select(".race, .race-card, .event, .race-item, section, article")
    if not blocks: blocks = [soup]

    for blk in blocks:
        txt = (blk.get_text(" ", strip=True) or "").lower()
        if not any(k in txt for k in ["race", "runner", "odds", "win", "place"]):
            continue

        # race header/time
        rtitle = None
        for h in blk.select("h2, h3, .race-title, .card-title"):
            ht = h.get_text(" ", strip=True)
            if re.search(r"\brace\s*\d+\b", ht, re.I):
                rtitle = ht; break

        rtime = None
        t = blk.find("time")
        if t: rtime = t.get_text(strip=True)

        runners = []
        rows = blk.select(".runner, .selection, .competitor, .bet-card, li, .row")
        for row in rows:
            text = " ".join(row.stripped_strings)
            if not text: continue

            # box / number
            box = None
            mbox = re.search(r"\b(Box|No\.?|#)\s*(\d{1,2})\b", text, re.I)
            if mbox: box = mbox.group(2)

            # name
            name_el = row.select_one(".runner-name, .name, a, [data-runner-name]")
            name = (name_el.get_text(" ", strip=True) if name_el else None)
            if not name:
                parts = [p for p in text.split() if len(p) > 2]
                name = " ".join(parts[:3]) if parts else text[:30]

            # odds (decimal)
            odds = None
            odds_el = row.select_one(".odds, .price, .win-price, [data-odds]")
            if odds_el: odds = odds_el.get_text(strip=True)
            else:
                m = re.search(r"\b(\d{1,2}\.\d)\b", text)
                if m: odds = m.group(1)

            if name:
                runners.append({"box": box, "runner": name, "odds": odds})

        if runners:
            races.append({"race": rtitle or "Race", "time": rtime, "runners": runners})

    return {"meeting": meeting_name, "url": url, "races": races}

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def write_csv(path: str, flat: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    hdr = ["meeting", "race", "time", "box", "runner", "odds", "meeting_url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in flat:
            w.writerow(r)

def main(out_dir: str = "data/thedogs") -> int:
    os.makedirs(out_dir, exist_ok=True)
    stamp = ts()

    meetings = scrape_meetings()
    write_json(os.path.join(out_dir, f"meetings_{stamp}.json"),
               {"fetched_at_utc": stamp, "count": len(meetings), "meetings": meetings})
    print(f"[DOGS] meetings: {len(meetings)}")

    all_meetings, flat = [], []
    for m in meetings:
        try:
            parsed = parse_meeting(m["url"])
            all_meetings.append(parsed)
            for race in parsed.get("races", []):
                for run in race.get("runners", []):
                    flat.append({
                        "meeting": parsed.get("meeting"),
                        "race": race.get("race"),
                        "time": race.get("time"),
                        "box": run.get("box"),
                        "runner": run.get("runner"),
                        "odds": run.get("odds"),
                        "meeting_url": parsed.get("url"),
                    })
            time.sleep(0.3)
        except Exception as e:
            print(f"[warn] meeting failed: {m['url']} -> {e}")

    write_json(os.path.join(out_dir, f"full_day_{stamp}.json"), {"fetched_at_utc": stamp, "meetings": all_meetings})
    write_csv(os.path.join(out_dir, f"full_day_{stamp}.csv"), flat)
    print(f"[DOGS] races rows: {len(flat)}")
    return 0

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/thedogs")
    args = ap.parse_args()
    raise SystemExit(main(args.out_dir))

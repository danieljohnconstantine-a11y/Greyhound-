# src/rns_daily.py
# Racing & Sports (R&S) daily AU greyhound form scraper
# Outputs:
#   data/rns/meetings_<TS>.json
#   data/rns/full_day_<TS>.json
#   data/rns/full_day_<TS>.csv

from __future__ import annotations
import os, re, csv, json, time
from datetime import datetime, timezone
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup

BASE = "https://www.racingandsports.com.au"
TODAY = f"{BASE}/form-guide/greyhound"

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/127 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }, timeout=timeout)
    r.raise_for_status()
    return r.text

def absol(href: str) -> str:
    if href.startswith("http"): return href
    if href.startswith("/"): return BASE + href
    return href

def scrape_meetings() -> List[Dict[str, str]]:
    html = get(TODAY)
    soup = BeautifulSoup(html, "lxml")
    meetings = []
    for a in soup.select("a[href*='/form-guide/greyhound/']"):
        name = (a.get_text(" ", strip=True) or "").strip()
        href = absol(a.get("href") or "")
        # filter obvious nav noise
        if not name or "/pdf-download" in href or href.endswith("/greyhound"):
            continue
        # prefer Australia meetings
        if "/australia/" not in href.lower():
            continue
        meetings.append({"name": name, "url": href})
    # de-dupe by url
    seen, out = set(), []
    for m in meetings:
        if m["url"] in seen: continue
        seen.add(m["url"]); out.append(m)
    return out

def parse_meeting(url: str) -> Dict[str, Any]:
    html = get(url)
    soup = BeautifulSoup(html, "lxml")

    title = soup.select_one("h1")
    meeting_name = (title.get_text(" ", strip=True) if title else url)

    races: List[Dict[str, Any]] = []
    # R&S usually groups races in tables/sections; be liberal:
    blocks = soup.select("section, article, .card, .panel, div.table-responsive, .race")
    if not blocks: blocks = [soup]  # fallback scan whole page

    for blk in blocks:
        blk_txt = (blk.get_text(" ", strip=True) or "").lower()
        if not any(k in blk_txt for k in ["race", "runner", "dog"]):
            continue

        # race header
        rtitle = None
        for h in blk.select("h2, h3, .card-title, .panel-title"):
            ht = h.get_text(" ", strip=True)
            if re.search(r"\brace\s*\d+\b", ht, re.I):
                rtitle = ht; break

        # rows of runners
        runners = []
        # common table layouts
        rows = blk.select("table tr")
        if not rows:
            rows = blk.select(".row, li, .list-group-item")

        for row in rows:
            text = (row.get_text(" ", strip=True) or "")
            if not text: continue
            # Try to pick fields
            box = None
            mbox = re.search(r"\b(Box|No\.?|#)\s*(\d{1,2})\b", text, re.I)
            if mbox: box = mbox.group(2)

            # Dog name
            name_el = row.select_one("a, .runner, .name, .horse, .greyhound")
            dog = (name_el.get_text(" ", strip=True) if name_el else None)
            if not dog:
                # fallback: longest token sequence
                parts = [p for p in text.split() if len(p) > 2]
                dog = " ".join(parts[:3]) if parts else text[:32]

            # Trainer (best effort)
            trainer = None
            mtr = re.search(r"Trainer[:\s]+([A-Za-z .'-]+)", text, re.I)
            if mtr: trainer = mtr.group(1).strip()

            # Last form (e.g. 12347)
            last_form = None
            mform = re.search(r"\b([1-8\-xX]{3,})\b", text)
            if mform: last_form = mform.group(1)

            # Best time (if shown)
            best_time = None
            mtime = re.search(r"\b(\d{2}\.\d{2})\b", text)
            if mtime: best_time = mtime.group(1)

            # Grade/Distance
            grade = None
            mgrade = re.search(r"\b(GR[ADE]*|GRADE)\s*([A-Z0-9-]+)\b", text, re.I)
            if mgrade: grade = mgrade.group(2)

            if dog:
                runners.append({
                    "box": box,
                    "runner": dog,
                    "trainer": trainer,
                    "last_form": last_form,
                    "best_time": best_time,
                    "grade": grade,
                })

        if runners:
            races.append({"race": rtitle or "Race", "runners": runners})

    return {"meeting": meeting_name, "url": url, "races": races}

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def write_csv(path: str, flat: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    hdr = ["meeting", "race", "box", "runner", "trainer", "last_form", "best_time", "grade", "meeting_url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in flat:
            w.writerow(r)

def main(out_dir: str = "data/rns") -> int:
    os.makedirs(out_dir, exist_ok=True)
    stamp = ts()

    meetings = scrape_meetings()
    write_json(os.path.join(out_dir, f"meetings_{stamp}.json"),
               {"fetched_at_utc": stamp, "count": len(meetings), "meetings": meetings})
    print(f"[RNS] meetings: {len(meetings)}")

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
                        "box": run.get("box"),
                        "runner": run.get("runner"),
                        "trainer": run.get("trainer"),
                        "last_form": run.get("last_form"),
                        "best_time": run.get("best_time"),
                        "grade": run.get("grade"),
                        "meeting_url": parsed.get("url"),
                    })
            time.sleep(0.3)
        except Exception as e:
            print(f"[warn] meeting failed: {m['url']} -> {e}")

    write_json(os.path.join(out_dir, f"full_day_{stamp}.json"), {"fetched_at_utc": stamp, "meetings": all_meetings})
    write_csv(os.path.join(out_dir, f"full_day_{stamp}.csv"), flat)
    print(f"[RNS] races rows: {len(flat)}")
    return 0

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/rns")
    args = ap.parse_args()
    raise SystemExit(main(args.out_dir))

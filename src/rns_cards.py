# rns_cards.py
# Consume meetings_* JSON from rns_daily.py and scrape each meeting page
# for race cards (race number/time + runner/box/trainer/odds).
#
# Design:
# - Headless Chromium w/ AU locale/timezone + geolocation (same as daily).
# - Heuristic DOM parsing that tolerates layout changes.
# - Writes/extends one CSV file beside the JSON.
# - Never hard-fails on a single bad meeting/race: logs and continues.

from __future__ import annotations
import argparse
import csv
import json
import os
import re
from dataclasses import dataclass, asdict
from typing import Iterable, List, Dict, Any

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PNG_W = 1440
PNG_H = 900

@dataclass
class CardRow:
    meeting_name: str
    meeting_url: str
    race_no: str
    race_time: str
    box: str
    runner: str
    trainer: str
    odds: str

RACE_HEADING_RE = re.compile(r"\bRace\s*(\d{1,2})\b", re.I)
TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\s*(AM|PM)?)\b", re.I)
BOX_RE = re.compile(r"^\s*(Box|No\.?|Trap)\s*(\d{1,2})\s*$", re.I)

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_meetings(json_path: str) -> Dict[str, Any]:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def csv_path_for(json_path: str) -> str:
    base = os.path.basename(json_path).replace("meetings_", "full_day_").replace(".json", ".csv")
    return os.path.join(os.path.dirname(json_path), base)

def find_race_sections(page) -> List[Any]:
    """
    Try to locate ‘sections’ that correspond to races. We:
      1) collect blocks containing headings like 'Race 1'
      2) fall back to elements whose text matches that pattern
    Returns a list of element handles (locators) representing the block.
    """
    sections = []

    # First, try semantic sections
    for sel in ["section", "article", "div", "li"]:
        loc = page.locator(sel)
        count = min(loc.count(), 500)  # cap for performance
        for i in range(count):
            node = loc.nth(i)
            try:
                txt = node.inner_text(timeout=0)
            except Exception:
                continue
            if RACE_HEADING_RE.search(txt or ""):
                sections.append(node)

    # Remove near-duplicates by text hash
    out = []
    seen = set()
    for n in sections:
        try:
            t = n.inner_text(timeout=0)
        except Exception:
            continue
        key = hash(t[:4000])
        if key not in seen:
            seen.add(key)
            out.append(n)
    return out

def parse_rows_from_section(node) -> Iterable[Dict[str, str]]:
    """
    Very tolerant runner-table parsing:
      - look for tables OR lists OR repeated row-like divs
      - pull out box number, runner name, trainer and odds by regex/markup
    """
    rows = []

    # Try tables first
    tables = node.locator("table")
    for ti in range(min(tables.count(), 5)):
        table = tables.nth(ti)
        trs = table.locator("tr")
        for ri in range(1, trs.count()):  # skip header row if any
            tr = trs.nth(ri)
            tds = tr.locator("th,td")
            cols = []
            for ci in range(min(tds.count(), 8)):
                try:
                    cols.append((tds.nth(ci).inner_text() or "").strip())
                except Exception:
                    cols.append("")
            if not any(cols):
                continue
            # Heuristic mapping
            box = ""
            runner = ""
            trainer = ""
            odds = ""
            for c in cols:
                if not box:
                    m = re.search(r"\b(\d{1,2})\b", c)
                    if m and ("box" in c.lower() or len(c) <= 2):
                        box = m.group(1)
                if not runner and len(c) >= 2 and not re.search(r"\d+(\.\d+)?", c):
                    # likely a name (no numbers)
                    runner = c if len(c) <= 60 else ""
                if not trainer and "trainer" in " ".join(cols).lower():
                    # trainer often sits in a cell with 'Trainer' label
                    if "trainer" in c.lower():
                        # next column may be the name
                        idx = cols.index(c)
                        if idx + 1 < len(cols):
                            trainer = cols[idx + 1]
                if not odds and re.search(r"\d+(\.\d+)?", c) and len(c) <= 8:
                    odds = re.findall(r"\d+(?:\.\d+)?", c)[0]
            rows.append({"box": box, "runner": runner, "trainer": trainer, "odds": odds})

    # Fallback: row-like divs / lis
    lis = node.locator("li,div[role='row'],div[class*='row']")
    for i in range(min(lis.count(), 200)):
        li = lis.nth(i)
        try:
            line = " ".join(li.all_inner_texts()).strip()
        except Exception:
            continue
        if not line or len(line) < 3:
            continue
        # Extract like "1  Dog Name  Trainer X  3.20"
        box = ""
        m = re.match(r"\s*(\d{1,2})\b", line)
        if m:
            box = m.group(1)
        name = ""
        trainer = ""
        odds = ""
        # odds as last number with decimal
        o = re.findall(r"\d+(?:\.\d+)?", line)
        if o:
            odds = o[-1]
        # naive name slice: remove box & odds tokens
        tmp = re.sub(r"^\s*\d{1,2}\s*", "", line)
        tmp = re.sub(r"\d+(?:\.\d+)?\s*$", "", tmp).strip()
        # pick a middle-ish chunk as name
        parts = [p.strip() for p in re.split(r"\s{2,}|  +|\t+\|", tmp) if p.strip()]
        if parts:
            name = parts[0][:60]
        # trainer hint
        m2 = re.search(r"Trainer[:\s]+([A-Za-z .'-]{2,})", line, re.I)
        if m2:
            trainer = m2.group(1).strip()
        rows.append({"box": box, "runner": name, "trainer": trainer, "odds": odds})

    # clean
    out = []
    for r in rows:
        if any(r.values()):
            out.append({k: (v or "").strip() for k, v in r.items()})
    return out

def scrape_meeting(page, meeting_name: str, meeting_url: str) -> List[CardRow]:
    page.goto(meeting_url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_selector("a, table, section, article", timeout=15000)
        page.wait_for_timeout(2500)
    except PWTimeout:
        pass

    # Identify race sections
    sections = find_race_sections(page)
    rows: List[CardRow] = []

    for sec in sections:
        try:
            txt = sec.inner_text(timeout=0)
        except Exception:
            continue

        # Race number
        race_no = ""
        m = RACE_HEADING_RE.search(txt or "")
        if m:
            race_no = m.group(1)

        # Race time (first time-looking token in section)
        race_time = ""
        mt = TIME_RE.search(txt or "")
        if mt:
            race_time = mt.group(1)

        # Extract “rows” of runners
        for r in parse_rows_from_section(sec):
            rows.append(CardRow(
                meeting_name=meeting_name,
                meeting_url=meeting_url,
                race_no=race_no,
                race_time=race_time,
                box=r.get("box", ""),
                runner=r.get("runner", ""),
                trainer=r.get("trainer", ""),
                odds=r.get("odds", ""),
            ))
    return rows

def main(meetings_json: str) -> int:
    data = read_meetings(meetings_json)
    meetings = data.get("meetings", [])
    out_csv = csv_path_for(meetings_json)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Prepare CSV (append-safe; ensure header once)
    new_file = not os.path.exists(out_csv)
    f = open(out_csv, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=[
        "meeting_name","meeting_url","race_no","race_time","box","runner","trainer","odds"
    ])
    if new_file:
        writer.writeheader()

    total_rows = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])
        context = browser.new_context(
            locale="en-AU",
            timezone_id="Australia/Sydney",
            geolocation={"latitude": -33.8688, "longitude": 151.2093},
            permissions=["geolocation"],
            viewport={"width": PNG_W, "height": PNG_H},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "en-AU,en;q=0.9"},
        )
        page = context.new_page()

        for m in meetings:
            name = m.get("name", "").strip()
            url = m.get("url", "").strip()
            if not url:
                continue
            try:
                rows = scrape_meeting(page, name, url)
                for r in rows:
                    writer.writerow(asdict(r))
                total_rows += len(rows)
                print(f"[OK] {name}: {len(rows)} rows")
            except Exception as e:
                print(f"[WARN] {name}: {e}")

        context.close()
        browser.close()

    f.close()
    print(f"wrote {total_rows} rows -> {out_csv}")
    # Always succeed; your data tells the story.
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("meetings_json", help="Path to meetings_*.json produced by rns_daily.py")
    args = ap.parse_args()
    raise SystemExit(main(args.meetings_json))

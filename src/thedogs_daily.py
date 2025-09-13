#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# TheDogs daily program landing (public pages; structure may vary)
DOGS_BASE = "https://www.thedogs.com.au"
DOGS_PROGRAM = f"{DOGS_BASE}/racing"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

USE_PLAYWRIGHT_FALLBACK = True

def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def get_html(url: str, session: Optional[requests.Session] = None) -> requests.Response:
    s = session or requests.Session()
    hdrs = dict(HEADERS)
    hdrs["User-Agent"] = random.choice(UA_POOL)
    time.sleep(0.6 + random.random() * 0.6)
    return s.get(url, headers=hdrs, timeout=30)

def render_with_playwright(url: str) -> Dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        return {"status": 0, "error": f"playwright not available: {e}"}
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        page = browser.new_page(user_agent=random.choice(UA_POOL))
        page.set_extra_http_headers({"Accept-Language": "en-AU,en;q=0.9"})
        page.goto(url, wait_until="load", timeout=45000)
        page.wait_for_timeout(1500)
        html = page.content()
        png = page.screenshot(full_page=True)
        status = 200
        browser.close()
    return {"status": status, "html": html, "png": png}

@dataclass
class Runner:
    number: str
    name: str
    box: Optional[str] = None
    trainer: Optional[str] = None
    odds: Optional[str] = None

@dataclass
class Race:
    meeting: str
    meeting_url: str
    race_no: str
    time_local: Optional[str]
    runners: List[Runner]

def parse_index(html: str) -> List[Dict[str, str]]:
    """
    Pull meeting links from TheDogs racing index (best-effort resilient).
    """
    soup = BeautifulSoup(html, "lxml")
    meetings: List[Dict[str, str]] = []
    for a in soup.select("a[href*='/racing/']"):
        href = a.get("href") or ""
        name = a.get_text(" ", strip=True)
        if not href or len(name) < 3:
            continue
        if not href.startswith("http"):
            href = DOGS_BASE + href
        # Filter obvious nav duplicates
        if "/racing/" in href and "/news" not in href and "/tips" not in href:
            meetings.append({"name": name, "url": href})
    # de-dup
    out, seen = [], set()
    for m in meetings:
        if m["url"] not in seen:
            seen.add(m["url"])
            out.append(m)
    return out

def parse_meeting(html: str, url: str) -> List[Race]:
    soup = BeautifulSoup(html, "lxml")
    races: List[Race] = []
    meeting_name = (soup.find("h1") or soup.find("h2") or soup.title)
    meeting_name = meeting_name.get_text(" ", strip=True) if meeting_name else "Unknown meeting"

    blocks = soup.select("section, div")
    for blk in blocks:
        h = blk.find(["h2", "h3"])
        if not h:
            continue
        title = h.get_text(" ", strip=True)
        if "Race" not in title:
            continue
        race_no = title.split()[1] if len(title.split()) > 1 else title
        # time
        time_node = blk.find(string=lambda t: isinstance(t, str) and ":" in t and len(t) <= 10)
        time_local = time_node.strip() if time_node else None

        runners: List[Runner] = []
        rows = blk.select("tr") or blk.select("li")
        for row in rows:
            txt = row.get_text(" ", strip=True)
            if not txt:
                continue
            parts = txt.split()
            num, name, odds = None, None, None
            if parts and parts[0].isdigit():
                num = parts[0]
                # naive dog name guess up to next numeric or '$'
                cut = next((i for i,p in enumerate(parts[1:],1) if p.isdigit() or p.startswith("$")), len(parts))
                name = " ".join(parts[1:cut]).strip()
            for piece in parts[::-1]:
                if piece.startswith("$") or piece.replace(".", "", 1).isdigit():
                    odds = piece; break
            if num and name:
                runners.append(Runner(number=num, name=name, odds=odds))

        if runners:
            races.append(Race(meeting=meeting_name, meeting_url=url, race_no=str(race_no), time_local=time_local, runners=runners))
    return races

def main(out_dir: str) -> int:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    sess = requests.Session()
    r = get_html(DOGS_PROGRAM, session=sess)
    status = r.status_code
    html = r.text

    if status != 200:
        if USE_PLAYWRIGHT_FALLBACK:
            rendered = render_with_playwright(DOGS_PROGRAM)
            status = rendered.get("status", status)
            html = rendered.get("html", html)
            png = rendered.get("png")
            if png:
                (out / f"page_{ts}.png").write_bytes(png)
        # Save what we saw and carry on
        save_text(out / f"debug_{ts}.html", html)
        save_text(out / f"meetings_{ts}.json", json.dumps({"fetched_at_utc": ts, "status_code": status, "meetings": []}, indent=2))
        print(f"[dogs] status={status} meetings=0")
        return 0

    save_text(out / f"debug_{ts}.html", html)
    meetings = parse_index(html)

    all_races: List[Dict[str, Any]] = []
    ok_meetings = 0
    for m in meetings[:12]:
        try:
            rr = get_html(m["url"], session=sess)
            h2 = rr.text
            st = rr.status_code
            if st != 200 and USE_PLAYWRIGHT_FALLBACK:
                rendered = render_with_playwright(m["url"])
                h2 = rendered.get("html") or h2
            races = parse_meeting(h2, m["url"])
            for race in races:
                all_races.append(asdict(race))
            if races:
                ok_meetings += 1
        except Exception as e:
            print(f"[dogs WARN] {m['url']} -> {e}", file=sys.stderr)

    save_text(out / f"meetings_{ts}.json", json.dumps({
        "fetched_at_utc": ts,
        "source": "thedogs",
        "count_meetings": ok_meetings,
        "races": all_races
    }, indent=2))

    # CSV
    rows = []
    for r in all_races:
        for runner in r.get("runners", []):
            rows.append({
                "meeting": r.get("meeting"),
                "meeting_url": r.get("meeting_url"),
                "race_no": r.get("race_no"),
                "time_local": r.get("time_local"),
                "runner_no": runner.get("number"),
                "runner_name": runner.get("name"),
                "box": runner.get("box"),
                "odds": runner.get("odds"),
                "trainer": runner.get("trainer"),
            })
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(out / f"full_day_{ts}.csv", index=False)

    print(f"[dogs] status=200 meetings_parsed={ok_meetings} races={len(all_races)}")
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/thedogs")
    args = ap.parse_args()
    sys.exit(main(args.out_dir))

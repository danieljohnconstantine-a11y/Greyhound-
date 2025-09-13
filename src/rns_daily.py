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

# Optional headless render if static fetch hits anti-bot
USE_PLAYWRIGHT_FALLBACK = True

RNS_BASE = "https://www.racingandsports.com.au"
RNS_GREY_FORM = f"{RNS_BASE}/form-guide/greyhound"

UA_POOL = [
    # rotate a few common desktop UAs
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
    # UA is set dynamically
}


def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def get_html(url: str, session: Optional[requests.Session] = None) -> requests.Response:
    s = session or requests.Session()
    hdrs = dict(HEADERS)
    hdrs["User-Agent"] = random.choice(UA_POOL)
    # small polite delay
    time.sleep(0.7 + random.random() * 0.6)
    r = s.get(url, headers=hdrs, timeout=30)
    return r


def render_with_playwright(url: str) -> Dict[str, Any]:
    """
    Headless Chromium render. Returns dict with status, html, png bytes (optional).
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        return {"status": 0, "error": f"playwright not available: {e}"}

    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        page = browser.new_page(user_agent=random.choice(UA_POOL))
        page.set_extra_http_headers({"Accept-Language": "en-AU,en;q=0.9"})
        page.goto(url, wait_until="load", timeout=45000)

        # Wait a touch for dynamic content
        page.wait_for_timeout(1500)
        html = page.content()
        png = page.screenshot(full_page=True)
        status = page.evaluate("() => document.readyState")  # "complete"
        browser.close()
    return {"status": 200 if status == "complete" else 206, "html": html, "png": png}


@dataclass
class Runner:
    number: str
    name: str
    box: Optional[str] = None
    odds: Optional[str] = None
    trainer: Optional[str] = None


@dataclass
class Race:
    meeting: str
    meeting_url: str
    race_no: str
    time_local: Optional[str]
    runners: List[Runner]


def parse_meetings_index(html: str) -> List[Dict[str, str]]:
    """
    Parse the R&S greyhound daily index page for AU meetings (best-effort).
    Page structure can change; this is resilient but conservative.
    """
    soup = BeautifulSoup(html, "lxml")
    meetings: List[Dict[str, str]] = []
    # Look for links that contain '/form-guide/greyhound/' and an AU track hint.
    for a in soup.select("a[href*='/form-guide/greyhound/']"):
        href = a.get("href") or ""
        name = a.get_text(strip=True)
        if not href.startswith("http"):
            href = RNS_BASE + href
        # Filter likely meeting links (avoid generic nav links)
        if "/form-guide/greyhound/" in href and len(name) > 2:
            meetings.append({"name": name, "url": href})
    # De-dup more than needed
    seen = set()
    out = []
    for m in meetings:
        k = m["url"]
        if k not in seen:
            seen.add(k)
            out.append(m)
    return out


def parse_meeting_detail(html: str, meeting_url: str) -> List[Race]:
    soup = BeautifulSoup(html, "lxml")
    races: List[Race] = []

    # Heuristics: find race blocks
    race_blocks = soup.select("section, div")
    for blk in race_blocks:
        # identify race number
        title = blk.find(["h2", "h3"])
        if not title:
            continue
        title_text = title.get_text(" ", strip=True)
        if not title_text or "Race" not in title_text:
            continue

        race_no = title_text.split()[0].replace("Race", "").strip() if "Race" in title_text else title_text
        time_node = blk.find(string=lambda t: isinstance(t, str) and ":" in t and len(t) <= 10)
        time_local = time_node.strip() if time_node else None

        # runners table/list
        runners: List[Runner] = []
        rows = blk.select("tr") or blk.select("li")
        for row in rows:
            txt = row.get_text(" ", strip=True)
            if not txt:
                continue
            # quick pattern guesses: "1 Dog Name (Box X) ... Trainer Y ... $2.80"
            num = None
            name = None
            box = None
            odds = None
            trainer = None

            # try number + name
            parts = txt.split()
            if parts and parts[0].isdigit():
                num = parts[0]
                name = " ".join(parts[1:6])  # guess
            # odds sign
            for piece in parts[::-1]:
                if piece.startswith("$") or piece.replace(".", "", 1).isdigit():
                    odds = piece
                    break
            # basic fill
            if num and name:
                runners.append(Runner(number=num, name=name, box=box, odds=odds, trainer=trainer))

        if runners:
            # meeting name fallback from page <h1>
            h1 = soup.find("h1")
            meeting_name = h1.get_text(" ", strip=True) if h1 else "Unknown meeting"
            races.append(Race(meeting=meeting_name, meeting_url=meeting_url, race_no=str(race_no), time_local=time_local, runners=runners))

    return races


def main(out_dir: str) -> int:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = now_ts()

    session = requests.Session()
    resp = get_html(RNS_GREY_FORM, session=session)

    if resp.status_code != 200:
        # Try fallback headless render (anti-bot pages often 403)
        html = resp.text
        status = resp.status_code
        if USE_PLAYWRIGHT_FALLBACK:
            rendered = render_with_playwright(RNS_GREY_FORM)
            status = rendered.get("status", status)
            html = rendered.get("html", html)
            png = rendered.get("png")
            if png:
                (out / f"page_{ts}.png").write_bytes(png)
        save_text(out / f"debug_{ts}.html", html)
        # Write empty meetings but don’t crash
        save_text(out / f"meetings_{ts}.json", json.dumps({"fetched_at_utc": ts, "status_code": status, "meetings": []}, indent=2))
        print(f"status={status} meetings=0")
        return 0

    # Index OK — parse
    index_html = resp.text
    save_text(out / f"debug_{ts}.html", index_html)
    meetings = parse_meetings_index(index_html)

    all_races: List[Dict[str, Any]] = []
    ok_count = 0

    for m in meetings[:12]:  # cap a bit for safety
        try:
            r = get_html(m["url"], session=session)
            if r.status_code != 200 and USE_PLAYWRIGHT_FALLBACK:
                rendered = render_with_playwright(m["url"])
                html = rendered.get("html") or ""
                status = rendered.get("status", r.status_code)
            else:
                html = r.text
                status = r.status_code

            if not html:
                continue

            races = parse_meeting_detail(html, m["url"])
            for race in races:
                all_races.append(asdict(race))
            if races:
                ok_count += 1
        except Exception as e:
            # keep going
            print(f"[warn] meeting parse failed {m['url']}: {e}", file=sys.stderr)

    # Save JSON
    json_path = out / f"meetings_{ts}.json"
    payload = {
        "fetched_at_utc": ts,
        "source": "racingandsports",
        "count_meetings": ok_count,
        "races": all_races,
    }
    save_text(json_path, json.dumps(payload, indent=2))

    # Also flatten to CSV (meeting,race,time,runner,odds)
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
        csv_path = out / f"full_day_{ts}.csv"
        df.to_csv(csv_path, index=False)

    print(f"status=200 meetings_parsed={ok_count} races={len(all_races)}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/rns")
    args = ap.parse_args()
    sys.exit(main(args.out_dir))

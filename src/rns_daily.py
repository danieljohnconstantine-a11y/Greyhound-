#!/usr/bin/env python3
"""
Render + scrape daily Australian greyhound meetings.

Strategy
--------
1) Try Racing & Sports (RNS): https://www.racingandsports.com.au/form-guide/greyhound
   - Headless Chromium with AU timezone/locale.
   - Collect anchor tags that look like meeting pages.
2) If zero meetings, try TheDogs: https://www.thedogs.com.au/racing
3) Save *everything*:
   - debug_<ts>.html : raw HTML of meetings page we saw
   - page_<ts>.png   : screenshot
   - meetings_<ts>.json   : list of meetings with name+url+source
   - full_day_<ts>.csv    : header CSV (maybe empty rows initially)

This script favors debuggability over perfection: you’ll always get artifacts
to iterate selectors quickly without guesswork.
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
from datetime import datetime, timezone
from typing import List, Dict
import pandas as pd

from playwright.sync_api import sync_playwright


UTC_TS = lambda: datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

RNS_URL = "https://www.racingandsports.com.au/form-guide/greyhound"
DOGS_URL = "https://www.thedogs.com.au/racing"

A_LIKE_MEETING = re.compile(
    r"(greyhound|dogs?).*(meeting|angle|park|track|cup|heats?|final|race\s*1)",
    re.IGNORECASE,
)

def save_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def save_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def ensure_dir(d: str):
    os.makedirs(d, exist_ok=True)

def normalize_href(href: str) -> str:
    if not href:
        return ""
    # Strip JS voids and fragments
    if href.startswith("javascript:"):
        return ""
    return href.strip()

def collect_meetings(page, base_url: str, source: str) -> List[Dict]:
    """Generic anchor collector with broad heuristics + de-dup."""
    anchors = page.locator("a")
    hrefs = set()
    meetings: List[Dict] = []

    count = anchors.count()
    for i in range(min(count, 3000)):  # cap for safety
        try:
            href = anchors.nth(i).get_attribute("href")
            text = anchors.nth(i).inner_text(timeout=0).strip()
        except Exception:
            continue
        href = normalize_href(href)
        if not href:
            continue

        full = href
        if href.startswith("/"):
            from urllib.parse import urljoin
            full = urljoin(base_url, href)

        # Heuristic filters
        if "greyhound" in full.lower() or "dogs" in full.lower():
            if A_LIKE_MEETING.search(text) or "race-1" in full.lower() or "meeting" in full.lower():
                if full not in hrefs:
                    hrefs.add(full)
                    meetings.append({
                        "name": text[:120] or full[-120:],
                        "url": full,
                        "source": source,
                    })
    return meetings

def scrape_source(play, url: str, out_dir: str, source_key: str) -> Dict:
    ts = UTC_TS()
    browser = play.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    ctx = browser.new_context(
        locale="en-AU",
        timezone_id="Australia/Sydney",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    )
    page = ctx.new_page()
    status = None
    html = ""

    try:
        resp = page.goto(url, wait_until="networkidle", timeout=60000)
        status = getattr(resp, "status", lambda: None)()
        time.sleep(2)  # let lazy content settle

        # Some sites render content after user interaction; add a small scroll
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)

        # Save artifacts
        html = page.content()
        save_text(os.path.join(out_dir, f"debug_{source_key}_{ts}.html"), html)
        page.screenshot(path=os.path.join(out_dir, f"page_{source_key}_{ts}.png"), full_page=True)

        meetings = collect_meetings(page, url, source_key)
        return {
            "status": status or 0,
            "meetings": meetings,
            "ts": ts,
            "source": source_key,
        }
    finally:
        try:
            ctx.close()
            browser.close()
        except Exception:
            pass

def write_csv(out_dir: str, ts: str, rows: List[Dict]):
    csv_path = os.path.join(out_dir, f"full_day_{ts}.csv")
    cols = ["meeting", "race", "time", "box", "runner", "odds", "meeting_url", "source"]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return csv_path

def main(out_dir: str) -> int:
    ensure_dir(out_dir)
    with sync_playwright() as p:
        # Try RNS first
        rns = scrape_source(p, RNS_URL, out_dir, "rns")
        # Fallback to TheDogs if needed
        meetings = rns["meetings"]
        source_used = "rns"
        if not meetings:
            dogs = scrape_source(p, DOGS_URL, out_dir, "dogs")
            if dogs["meetings"]:
                meetings = dogs["meetings"]
                source_used = "dogs"

    ts = UTC_TS()
    out_json = os.path.join(out_dir, f"meetings_{ts}.json")
    save_json(out_json, {
        "fetched_at_utc": ts,
        "source_used": source_used,
        "meetings": meetings,
    })

    # Stub: just write header CSV for now (we’ll enrich races once we lock the meeting selectors)
    csv_path = write_csv(out_dir, ts, rows=[])

    print(f"status={(rns['status'])} source={source_used} meetings={len(meetings)}")
    print(f"wrote: {out_json}")
    print(f"csv:   {csv_path}")
    print("Saved debug HTML + screenshots alongside outputs.")
    # Non-zero exit if both sources produced nothing to signal we need to tweak selectors
    return 0 if meetings else 2

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/rns")
    args = ap.parse_args()
    raise SystemExit(main(args.out_dir))

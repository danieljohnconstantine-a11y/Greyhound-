# rns_daily.py
# Robust daily scraper for Racing & Sports (AU) greyhound meetings.
# - Uses Playwright headless Chromium with AU locale/timezone.
# - Tries both Form Guide and Greyhound home pages.
# - Waits for dynamic content, extracts meeting links broadly.
# - Writes JSON + CSV; warns (does not hard-fail) if zero meetings.

from __future__ import annotations
import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

FORM_URLS = [
    "https://www.racingandsports.com.au/form-guide/greyhound",  # main target
    "https://www.racingandsports.com.au/greyhound",             # fallback landing
]

OUT_SUBDIR = "rns"
PNG_W = 1440
PNG_H = 900

# Simple utilities
def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def looks_like_meeting(text: str, href: str) -> bool:
    """Broad heuristics for a meeting link."""
    t = (text or "").strip()
    h = (href or "").strip().lower()
    if not t or not h:
        return False
    # must be a racing & sports link and reference greyhound section
    if "racingandsports.com.au" not in h and not h.startswith("/"):
        return False
    if "greyhound" not in h:
        return False
    # discard obvious non-meeting links
    bad = ["results", "news", "help", "terms", "privacy", "contact", "bet", "odds"]  # conservative
    if any(b in h for b in bad):
        return False
    # meeting names are usually a few letters, not a whole paragraph
    if len(t) < 3 or len(t) > 60:
        return False
    return True

def absolutize(base: str, href: str) -> str:
    try:
        return urljoin(base, href)
    except Exception:
        return href

def scrape_meetings_with_playwright(out_dir: str) -> dict:
    ts = utc_stamp()
    out_dir_abs = os.path.abspath(out_dir)
    ensure_dir(out_dir_abs)

    debug_html_path = os.path.join(out_dir_abs, f"debug_{ts}.html")
    debug_png_path  = os.path.join(out_dir_abs, f"page_{ts}.png")

    meetings: list[dict] = []
    seen: set[tuple[str, str]] = set()
    final_status = 0
    last_html = ""

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
            extra_http_headers={
                "Accept-Language": "en-AU,en;q=0.9",
                "DNT": "1",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
            },
        )
        page = context.new_page()

        try:
            for url in FORM_URLS:
                # navigate and give the page time to render client content
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                # wait for any anchors to appear (content arrives via JS)
                page.wait_for_selector("a", timeout=15000)
                # extra breathing room for late JS
                page.wait_for_timeout(2500)

                # Grab content for debugging
                html = page.content()
                last_html = html
                # Snapshot only once (first page usually enough to understand blocking)
                if not os.path.exists(debug_png_path):
                    try:
                        page.screenshot(path=debug_png_path, full_page=True)
                    except Exception:
                        pass

                # Detect obvious block pages; keep status flag for the report
                lower = html.lower()
                if "access denied" in lower or "not available here" in lower or "403" in lower:
                    final_status = max(final_status, 403)  # remember we saw a block
                    continue  # try next URL

                # Collect anchors broadly, then filter
                anchors = page.locator("a")
                count = anchors.count()
                for i in range(count):
                    try:
                        href = anchors.nth(i).get_attribute("href") or ""
                        text = anchors.nth(i).inner_text().strip()
                    except Exception:
                        continue
                    if not href:
                        continue
                    if looks_like_meeting(text, href):
                        abs_url = absolutize(url, href)
                        key = (text.lower(), abs_url)
                        if key in seen:
                            continue
                        seen.add(key)
                        meetings.append({"name": text, "url": abs_url})

                # If we found meetings on this URL, great — stop trying others
                if meetings:
                    break

        finally:
            try:
                # Always save debug HTML for diagnosis
                with open(debug_html_path, "w", encoding="utf-8") as f:
                    f.write(last_html or "<!-- empty -->")
            except Exception:
                pass
            context.close()
            browser.close()

    # Sort + dedupe by name then url for stable outputs
    meetings.sort(key=lambda m: (m["name"].lower(), m["url"]))

    # Write JSON
    json_path = os.path.join(out_dir_abs, f"meetings_{ts}.json")
    data = {
        "fetched_at_utc": ts,
        "status_code": final_status or (200 if meetings else 206),
        "count": len(meetings),
        "meetings": meetings,
        "source_pages": FORM_URLS,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    # Write CSV (header only if none)
    csv_path = os.path.join(out_dir_abs, f"full_day_{ts}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["meeting_name", "meeting_url"])
        for m in meetings:
            writer.writerow([m["name"], m["url"]])

    return data

def main(out_dir: str) -> int:
    ensure_dir(out_dir)
    out_dir = os.path.join(out_dir, OUT_SUBDIR)
    ensure_dir(out_dir)

    report = scrape_meetings_with_playwright(out_dir)
    status = report.get("status_code", 0)
    count = report.get("count", 0)

    print(f"status={status} meetings={count}")
    print("wrote:", os.path.join(out_dir, f"meetings_{report['fetched_at_utc']}.json"))
    print("saved debug:", os.path.join(out_dir, f"debug_{report['fetched_at_utc']}.html"))

    # ⚠️ Do NOT hard-fail when zero meetings; return 0 and let subsequent steps keep running.
    # If you want the run to fail on zero found, change to `return 2 if count == 0 else 0`.
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data", help="parent output folder (default: data)")
    args = parser.parse_args()
    sys.exit(main(args.out_dir))

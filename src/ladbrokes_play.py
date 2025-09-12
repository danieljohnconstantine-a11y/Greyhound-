# src/ladbrokes_play.py
# Discover Ladbrokes greyhound meetings (AU) and extract races + runners + odds.
# Outputs:
#   data/ladbrokes/meetings_<TS>.json    -> discovered meetings
#   data/ladbrokes/full_day_<TS>.json    -> all meetings with races/odds
#   data/ladbrokes/full_day_<TS>.csv     -> flat CSV of meeting,race,runner,odds
#   data/ladbrokes/debug_<TS>.html       -> landing page HTML
#   data/ladbrokes/page_<TS>.png         -> landing page screenshot
#
# Requires: Playwright (Chromium). Works best from an AU IP.

from __future__ import annotations
import os, re, csv, json
from datetime import datetime, timezone
from typing import List, Dict, Any

from bs4 import BeautifulSoup  # pip install beautifulsoup4 lxml
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

GREYHOUNDS_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0.0.0 Safari/537.36")

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def absolutize(href: str) -> str:
    if not href:
        return href
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.ladbrokes.com.au" + href
    return href

def parse_meetings_from_html(html: str) -> List[Dict[str, str]]:
    """Heuristic extraction of meeting links from landing HTML."""
    soup = BeautifulSoup(html, "lxml")
    found: List[Dict[str, str]] = []

    # Try precise first: links that clearly look like meetings
    selectors = [
        "a[href*='/racing/greyhound-racing/']",
        "a[href*='/racing/greyhound/']",
        "a[href*='/greyhound-racing/']",
        "section a[href*='/racing/']",
    ]
    seen = set()
    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href") or ""
            name = a.get_text(" ", strip=True) or ""
            if not href or not name:
                continue
            name_low = name.lower()
            if any(k in name_low for k in ["greyhound", "race", "park", "track"]):
                url = absolutize(href)
                if url not in seen:
                    seen.add(url)
                    found.append({"name": name, "url": url})

    # Fallback regex over anchors if needed
    if not found:
        for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.I | re.S):
            href = m.group(1)
            label = re.sub(r"<[^>]+>", "", m.group(2)).strip()
            if not href or not label:
                continue
            low = label.lower()
            if any(k in low for k in ["greyhound", "race", "park", "track"]):
                url = absolutize(href)
                if url not in seen:
                    seen.add(url)
                    found.append({"name": label, "url": url})

    return found

def parse_race_block_html(block) -> Dict[str, Any]:
    """Extract race title/time and runner odds from a single race-like block."""
    # Race title / header (best effort)
    race_title = None
    header = block.select_one("h2, h3, .race-title, .event-title, .card-title")
    if header:
        race_title = header.get_text(" ", strip=True)

    # Race time (best effort)
    race_time = None
    t = block.find("time")
    if t:
        race_time = t.get_text(strip=True)

    # Runners: try common classes first, then generic items
    runners = []
    # Common Ladbrokes-like classes (varies often)
    runner_nodes = block.select(
        ".runner, .selection, .selection-card, .bet-card, "
        ".competitor, .runner-card, .participant"
    )
    if not runner_nodes:
        # Fallback: list items or rows
        runner_nodes = block.select("li, .row, .grid > *")

    for rn in runner_nodes:
        text = " ".join(rn.stripped_strings)
        if not text or len(text) < 2:
            continue
        # Runner name: try specific elements first
        name_el = rn.select_one(
            ".runner-name, .selection-name, .name, [data-runner-name]"
        )
        if name_el:
            name = name_el.get_text(" ", strip=True)
        else:
            # fallback: pick a reasonably long token from text
            parts = [p for p in text.split() if len(p) > 2]
            name = " ".join(parts[:3]) if parts else text[:30]

        # Box number (if visible)
        box = None
        box_el = rn.select_one(".box, .draw, .barrier, .runner-number")
        if box_el:
            box = box_el.get_text(strip=True)
        else:
            m_box = re.search(r"\b(Box|No\.?|#)\s*(\d{1,2})\b", text, re.I)
            if m_box:
                box = m_box.group(2)

        # Odds (decimal)
        odds = None
        odds_el = rn.select_one(".odds, .price, .win-price, [data-odds]")
        if odds_el:
            odds = odds_el.get_text(strip=True)
        else:
            m = re.search(r"\b(\d{1,2}\.\d)\b", text)
            if m:
                odds = m.group(1)

        # Skip if clearly not a runner row (very short labels etc)
        if len(name.strip()) < 2:
            continue
        runners.append({"box": box, "runner": name.strip(), "odds": odds})

    return {
        "race": race_title or "Race",
        "time": race_time,
        "runners": runners,
    }

def parse_meeting_page(html: str, meeting_url: str) -> Dict[str, Any]:
    """Parse a meeting page into races with runner odds."""
    soup = BeautifulSoup(html, "lxml")
    # Meeting title (best effort)
    title_el = soup.select_one("h1, .meeting-title, .page-title")
    meeting_name = title_el.get_text(" ", strip=True) if title_el else meeting_url

    # Race containers: broad sweep of plausible wrappers
    race_blocks = soup.select(
        ".race-card, .event-card, .race, .race-item, .race-container, section, article"
    )
    races: List[Dict[str, Any]] = []

    # If no obvious blocks found, try to group by headings:
    if not race_blocks:
        headings = soup.select("h2, h3")
        for h in headings:
            # Take the heading and some following siblings as a 'block'
            parent = h.find_parent()
            if parent:
                races.append(parse_race_block_html(parent))
        return {"meeting": meeting_name, "url": meeting_url, "races": races}

    for blk in race_blocks:
        # Heuristic: must mention something race-like
        blk_text = (blk.get_text(" ", strip=True) or "").lower()
        if not any(k in blk_text for k in ["race", "runner", "odds", "win", "place"]):
            continue
        races.append(parse_race_block_html(blk))

    # Remove empties
    races = [r for r in races if r.get("runners")]
    return {"meeting": meeting_name, "url": meeting_url, "races": races}

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    hdr = ["meeting", "race", "time", "box", "runner", "odds", "meeting_url"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main(out_dir: str = "data/ladbrokes") -> int:
    os.makedirs(out_dir, exist_ok=True)
    stamp = ts()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1366, "height": 900},
            timezone_id="Australia/Sydney",
            locale="en-AU",
            geolocation={"latitude": -33.8688, "longitude": 151.2093},
            permissions=["geolocation"],
        )
        page = ctx.new_page()

        # 1) LANDING: open greyhound racing page
        resp = page.goto(GREYHOUNDS_URL, wait_until="networkidle", timeout=60000)
        status = resp.status if resp else 0

        # Accept cookie banner if present (best-effort)
        for sel in [
            'button:has-text("Accept")',
            'button:has-text("I Accept")',
            '[data-testid="cookie-accept"]',
            'button:has-text("Agree")',
        ]:
            try:
                if page.locator(sel).first.is_visible():
                    page.locator(sel).first.click(timeout=1500)
                    page.wait_for_timeout(400)
                    break
            except Exception:
                pass

        # Give time for lazy content
        page.wait_for_timeout(1500)
        for _ in range(3):
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(400)

        landing_html = page.content()
        # Save debug artifacts
        with open(os.path.join(out_dir, f"debug_{stamp}.html"), "w", encoding="utf-8") as f:
            f.write(landing_html)
        page.screenshot(path=os.path.join(out_dir, f"page_{stamp}.png"), full_page=True)

        # 2) MEETINGS: extract meeting links
        meetings = parse_meetings_from_html(landing_html)
        meetings_json_path = os.path.join(out_dir, f"meetings_{stamp}.json")
        write_json(meetings_json_path, {
            "fetched_at_utc": stamp,
            "status_code": status,
            "count": len(meetings),
            "meetings": meetings,
        })
        print(f"[meetings] status={status} count={len(meetings)} -> {meetings_json_path}")

        # 3) SCRAPE EACH MEETING: races + odds
        all_meetings: List[Dict[str, Any]] = []
        flat_rows: List[Dict[str, Any]] = []

        # Use a fresh tab for meetings to keep landing page around if needed
        mpage = ctx.new_page()
        for m in meetings[:30]:  # safety cap
            url = m.get("url") or ""
            if not url:
                continue
            try:
                mresp = mpage.goto(url, wait_until="networkidle", timeout=60000)
                mpage.wait_for_timeout(1000)
                mhtml = mpage.content()
                parsed = parse_meeting_page(mhtml, url)
                all_meetings.append(parsed)

                # Flatten for CSV
                meeting_name = parsed.get("meeting", "")
                for race in parsed.get("races", []):
                    rtitle = race.get("race", "")
                    rtime  = race.get("time", "")
                    for run in race.get("runners", []):
                        flat_rows.append({
                            "meeting": meeting_name,
                            "race": rtitle,
                            "time": rtime,
                            "box": run.get("box"),
                            "runner": run.get("runner"),
                            "odds": run.get("odds"),
                            "meeting_url": url,
                        })

                # polite pause
                mpage.wait_for_timeout(400)
            except PWTimeout:
                print(f"[warn] timeout meeting: {url}")
            except Exception as e:
                print(f"[warn] error meeting: {url} -> {e}")

        ctx.close()
        browser.close()

    # 4) Save full-day outputs
    full_json = os.path.join(out_dir, f"full_day_{stamp}.json")
    full_csv  = os.path.join(out_dir, f"full_day_{stamp}.csv")
    write_json(full_json, {"fetched_at_utc": stamp, "meetings": all_meetings})
    write_csv(full_csv, flat_rows)

    print(f"[done] meetings={len(all_meetings)} rows={len(flat_rows)}")
    print(f"[out] {full_json}")
    print(f"[out] {full_csv}")
    return 0

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    raise SystemExit(main(args.out_dir))

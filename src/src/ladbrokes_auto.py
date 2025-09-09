# src/ladbrokes_auto.py
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

from playwright.async_api import async_playwright, TimeoutError as PWTimeout


DISCOVERY_URLS = [
    # Ladbrokes greyhounds landing; scraper will mine it for all meeting + race links
    "https://www.ladbrokes.com.au/racing/greyhounds",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = int(os.getenv("TIMEOUT_SECS", "30")) * 1000  # ms

MEETING_HREF_RE = re.compile(r"/racing/greyhounds/[^/]+/?$", re.I)
RACE_HREF_RE    = re.compile(r"/racing/greyhounds/[^/]+/race-\d+$", re.I)
PRICE_RE        = re.compile(r"\$\s*\d+(?:[.,]\d+)?")

@dataclass
class Outcome:
    runner: str
    price: float | None

@dataclass
class Race:
    meeting_name: str
    race_label: str
    race_url: str
    outcomes: List[Outcome]

@dataclass
class Meeting:
    meeting_name: str
    meeting_url: str
    races: List[Race]
    note: str = ""


async def _extract_links(page) -> List[str]:
    links = []
    for a in await page.locator("a").all():
        try:
            href = await a.get_attribute("href")
            text = (await a.text_content()) or ""
        except Exception:
            continue
        if not href:
            continue
        # normalize to absolute
        if href.startswith("/"):
            href = "https://www.ladbrokes.com.au" + href
        if href.startswith("https://www.ladbrokes.com.au/racing/greyhounds"):
            links.append(href)
    # dedupe
    out = []
    seen = set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


async def discover_meetings(context) -> List[str]:
    meetings: List[str] = []
    page = await context.new_page()
    await page.route("**/*", lambda route: route.continue_())
    await page.set_extra_http_headers({"User-Agent": USER_AGENT})
    for url in DISCOVERY_URLS:
        try:
            await page.goto(url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT)
            await page.wait_for_timeout(1500)
            links = await _extract_links(page)
            # meeting pages often look like /racing/greyhounds/<track>
            for u in links:
                if MEETING_HREF_RE.search(u):
                    meetings.append(u.rstrip("/"))
        except PWTimeout:
            print(f"[warn] Discovery timeout: {url}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] Discovery failed: {url} -> {e}", file=sys.stderr)
    # dedupe
    meetings = sorted(set(meetings))
    return meetings


async def discover_races(context, meeting_url: str) -> List[str]:
    page = await context.new_page()
    await page.set_extra_http_headers({"User-Agent": USER_AGENT})
    races: List[str] = []
    try:
        await page.goto(meeting_url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT)
        await page.wait_for_timeout(1500)
        links = await _extract_links(page)
        # race URLs like .../race-1, race-2
        for u in links:
            if RACE_HREF_RE.search(u):
                races.append(u)
    except Exception as e:
        print(f"[warn] Race discovery failed for {meeting_url}: {e}", file=sys.stderr)
    races = sorted(set(races))
    return races


async def scrape_race(context, race_url: str) -> Race | None:
    page = await context.new_page()
    await page.set_extra_http_headers({"User-Agent": USER_AGENT})
    meeting_name = ""
    race_label = ""
    outcomes: List[Outcome] = []

    try:
        await page.goto(race_url, wait_until="networkidle", timeout=DEFAULT_TIMEOUT)
        await page.wait_for_timeout(1500)

        # Try to infer meeting/race labels from the visible header text
        title_text = (await page.title()) or ""
        # e.g., "Warragul Race 1 | Greyhounds | Ladbrokes"
        m = re.search(r"([A-Za-z \-']+)\s+Race\s+(\d+)", title_text)
        if m:
            meeting_name = m.group(1).strip()
            race_label = f"Race {m.group(2)}"

        # Primary attempt: detect runner rows with a price token like "$2.40"
        rows = await page.locator("div, li, tr").all()
        for r in rows:
            try:
                t = (await r.inner_text()).strip()
            except Exception:
                continue
            if not t or "$" not in t:
                continue
            pm = PRICE_RE.findall(t)
            if not pm:
                continue
            # choose the last price instance in the row
            price_str = pm[-1]
            try:
                price = float(price_str.replace("$", "").replace(",", ""))
            except Exception:
                price = None
            name = t.replace(price_str, "").strip()
            # Basic cleanup: collapse repeated spaces
            name = re.sub(r"\s{2,}", " ", name)
            # Filter out obviously bad lines (too short or price-only)
            if name and len(name) > 2:
                outcomes.append(Outcome(runner=name, price=price))

        # Deduplicate runners by name, keep the first price seen
        seen_names = set()
        unique_outcomes: List[Outcome] = []
        for o in outcomes:
            key = o.runner.lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            unique_outcomes.append(o)

        return Race(
            meeting_name=meeting_name or "Unknown Meeting",
            race_label=race_label or "Race",
            race_url=race_url,
            outcomes=unique_outcomes,
        )
    except Exception as e:
        print(f"[warn] Failed to scrape race {race_url}: {e}", file=sys.stderr)
        return None


async def main(out_dir: str):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)

        # 1) Find all meetings today
        meetings = await discover_meetings(context)
        print(f"[info] discovered meetings: {len(meetings)}")

        meetings_data: List[Meeting] = []

        # 2) For each meeting, discover races, then scrape each race
        for m_url in meetings:
            race_urls = await discover_races(context, m_url)
            print(f"[info] {m_url} -> races: {len(race_urls)}")

            races: List[Race] = []
            for r_url in race_urls:
                race = await scrape_race(context, r_url)
                if race and race.outcomes:
                    races.append(race)

            meetings_data.append(Meeting(
                meeting_name=(races[0].meeting_name if races else ""),
                meeting_url=m_url,
                races=races,
                note="auto-discovery",
            ))

        await browser.close()

    # Write JSON
    json_file = out_path / "greyhound_odds.json"
    json_payload: Dict[str, Any] = {
        "source": "ladbrokes",
        "discovery_urls": DISCOVERY_URLS,
        "meetings": [
            {
                "meeting_name": m.meeting_name,
                "meeting_url": m.meeting_url,
                "note": m.note,
                "races": [
                    {
                        "meeting_name": r.meeting_name,
                        "race_label": r.race_label,
                        "race_url": r.race_url,
                        "outcomes": [asdict(o) for o in r.outcomes],
                    } for r in m.races
                ],
            } for m in meetings_data
        ],
    }
    json_file.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
    print(f"[ok] wrote {json_file}")

    # Write CSV (flat)
    import pandas as pd
    rows = []
    for m in meetings_data:
        for r in m.races:
            for o in r.outcomes:
                rows.append({
                    "meeting": m.meeting_name or r.meeting_name,
                    "meeting_url": m.meeting_url,
                    "race": r.race_label,
                    "race_url": r.race_url,
                    "runner": o.runner,
                    "price": o.price,
                })
    csv_file = out_path / "greyhound_odds.csv"
    pd.DataFrame(rows).to_csv(csv_file, index=False)
    print(f"[ok] wrote {csv_file}")


if __name__ == "__main__":
    # --out-dir data
    out = "data"
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--out-dir" and i + 1 < len(args):
            out = args[i + 1]
    asyncio.run(main(out))

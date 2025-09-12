# rns_daily.py
# Tier 2 – Use Playwright (Chromium) to render Racing & Sports greyhound form page

from __future__ import annotations
import os, json, argparse, datetime as dt
from playwright.sync_api import sync_playwright

BASE = "https://www.racingandsports.com.au"
TODAY = f"{BASE}/form-guide/greyhound"

def utc_stamp() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def scrape_meetings() -> tuple[dict, str]:
    stamp = utc_stamp()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/129.0.0.0 Safari/537.36"
            )
        )
        page.goto(TODAY, timeout=60000)
        page.wait_for_timeout(5000)  # wait for JS and races to load

        html = page.content()

        # Extract meeting links
        meetings = []
        anchors = page.locator("a").all()
        seen = set()
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                text = a.inner_text().strip()
            except:
                continue
            if not href or "greyhound" not in href:
                continue
            if any(bad in href.lower() for bad in ["results", "terms", "privacy"]):
                continue
            key = (text.lower(), href)
            if key in seen:
                continue
            seen.add(key)
            if len(text) >= 3:
                meetings.append({"name": text, "url": href})

        browser.close()

    data = {
        "fetched_at_utc": stamp,
        "source_url": TODAY,
        "status_code": 200 if meetings else 206,
        "count": len(meetings),
        "meetings": meetings,
        "note": "Rendered with Playwright Chromium to bypass bot protection",
    }
    return data, html

def write_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def write_json(path: str, obj: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)

def main(out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    data, html = scrape_meetings()
    stamp = data["fetched_at_utc"]

    debug_path = os.path.join(out_dir, f"debug_{stamp}.html")
    out_json = os.path.join(out_dir, f"meetings_{stamp}.json")

    write_text(debug_path, html)
    write_json(out_json, data)

    print(f"status={data['status_code']} meetings={data['count']}")
    print(f"wrote: {out_json}")
    print(f"saved debug: {debug_path}")

    return 0 if data["count"] > 0 else 2

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data/rns")
    args = parser.parse_args()
    raise SystemExit(main(args.out_dir))

import os
import json
from datetime import datetime
from playwright.sync_api import sync_playwright

URL = "https://thedogs.com.au/racing"

def fetch_page(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0.0.0 Safari/537.36"
        )
        page.goto(url, timeout=60000)
        html = page.content()
        browser.close()
        return html

def main(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    fetched_at = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    try:
        html = fetch_page(URL)
        debug_file = os.path.join(out_dir, f"debug_{fetched_at}.html")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(html)

        json_file = os.path.join(out_dir, f"meetings_{fetched_at}.json")
        data = {
            "fetched_at_utc": fetched_at,
            "url": URL,
            "meetings": []
        }
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"[thedogs] saved debug={debug_file}, json={json_file}")

    except Exception as e:
        print(f"[thedogs] error: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()
    main(args.out_dir)

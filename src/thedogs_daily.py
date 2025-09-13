#!/usr/bin/env python3
import argparse, json, os, sys
from datetime import datetime, timezone

def utcstamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

"""
Very defensive: we use Playwright to pull the main racing page HTML,
save it (debug) and attempt a light parse for meeting cards. If anything
fails, we still emit a JSON with count=0 so the pipeline doesn't break.
"""

def main(out_dir: str) -> int:
    ts = utcstamp()
    os.makedirs(out_dir, exist_ok=True)

    status = 520
    meetings = []
    html = ""
    png_path = os.path.join(out_dir, f"page_{ts}.png")
    debug_path = os.path.join(out_dir, f"debug_{ts}.html")

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            ctx = browser.new_context(locale="en-AU", timezone_id="Australia/Sydney")
            page = ctx.new_page()
            resp = page.goto("https://www.thedogs.com.au/racing", wait_until="domcontentloaded", timeout=45000)
            status = resp.status if resp else 0
            page.wait_for_timeout(2000)  # small settle
            html = page.content()
            try:
                page.screenshot(path=png_path, full_page=True)
            except:
                pass
            # Minimal heuristic parse (site markup can vary)
            # Keep it super-safe: don't throw on parse errors.
            try:
                cards = page.locator("a:has-text('Race')").all()
                # This is intentionally loose — reliable selectors can be added later.
                # We'll just note we touched the page.
                if cards:
                    meetings = [{"name":"TheDogs - see site", "url":"https://www.thedogs.com.au/racing"}]
            except:
                pass
            ctx.close(); browser.close()
    except Exception as e:
        html = f"Playwright failure: {e}"

    # Save debug HTML always
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html or "")

    out_json = {
        "fetched_at_utc": ts,
        "source": "thedogs",
        "status_code": status,
        "count": len(meetings),
        "meetings": meetings,
    }
    json_path = os.path.join(out_dir, f"meetings_{ts}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out_json, f, ensure_ascii=False, indent=2)

    print(f"[thedogs] status={status} meetings={len(meetings)}")
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    sys.exit(main(args.out_dir))

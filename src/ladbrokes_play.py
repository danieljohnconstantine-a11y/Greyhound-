# src/ladbrokes_play.py
import os
import json
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

LADBROKES_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def main(out_dir="data/ladbrokes"):
    os.makedirs(out_dir, exist_ok=True)
    ts = now_utc()

    meetings = []
    status_code = 0
    debug_html_path = os.path.join(out_dir, f"debug_{ts}.html")
    out_json_path   = os.path.join(out_dir, f"meetings_{ts}.json")

    cookie_raw = os.getenv("LADBROKES_COOKIE", "").strip()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1366, "height": 900},
            locale="en-AU",
            geolocation={"latitude": -33.8688, "longitude": 151.2093}, # Sydney
            permissions=["geolocation"],
        )

        # Optional cookie support (copy/paste from your own browser)
        if cookie_raw:
            # Cookie string "name=value; name2=value2" → list of dicts
            cookies = []
            for pair in cookie_raw.split(";"):
                if "=" in pair:
                    k, v = pair.strip().split("=", 1)
                    cookies.append({
                        "name": k.strip(),
                        "value": v.strip(),
                        "domain": ".ladbrokes.com.au",
                        "path": "/",
                        "httpOnly": False,
                        "secure": True,
                    })
            if cookies:
                ctx.add_cookies(cookies)

        page = ctx.new_page()
        try:
            resp = page.goto(LADBROKES_URL, wait_until="domcontentloaded", timeout=45000)
            status_code = resp.status if resp else 0

            # Let client-side scripts run
            page.wait_for_timeout(3000)

            # Accept cookies / close banners if present (best-effort, ignore if missing)
            for sel in [
                'button:has-text("Accept")',
                'button:has-text("I Accept")',
                'button:has-text("Agree")',
                '[data-testid="cookie-accept"]',
            ]:
                try:
                    if page.locator(sel).first.is_visible():
                        page.locator(sel).first.click(timeout=1000)
                        page.wait_for_timeout(500)
                        break
                except Exception:
                    pass

            # Heuristic: gather meeting-like anchors
            anchors = page.locator("a").all()
            for a in anchors:
                try:
                    label = a.inner_text().strip()
                    href  = a.get_attribute("href") or ""
                except Exception:
                    continue
                if not label or not href:
                    continue
                low = label.lower()
                if any(x in low for x in ["race ", "racing", "greyhound", "dogs", "meeting", "park", "track"]):
                    if href.startswith("/"):
                        href = "https://www.ladbrokes.com.au" + href
                    meetings.append({"name": label, "url": href})
        finally:
            # Save debug HTML no matter what
            try:
                html = page.content()
                with open(debug_html_path, "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass
            ctx.close()
            browser.close()

    # Deduplicate by URL
    seen = set(); dedup = []
    for m in meetings:
        if m["url"] in seen: continue
        seen.add(m["url"]); dedup.append(m)

    payload = {
        "fetched_at_utc": ts,
        "source": "playwright",
        "status_code": status_code,
        "count": len(dedup),
        "meetings": dedup,
    }
    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"status={status_code} meetings={len(dedup)}")
    print(f"wrote: {out_json_path}")
    print(f"saved debug: {debug_html_path}")

if __name__ == "__main__":
    import argparse, os
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes")
    args = ap.parse_args()
    main(args.out_dir)

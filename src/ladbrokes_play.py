# src/ladbrokes_play.py
import os, re, json
from datetime import datetime, timezone
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

LADBROKES_URL = "https://www.ladbrokes.com.au/racing/greyhound-racing"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def absolutize(href: str) -> str:
    if href.startswith("http"): return href
    if href.startswith("/"):    return "https://www.ladbrokes.com.au" + href
    return href

def extract_meetings_html(html: str) -> List[Dict]:
    """Emergency fallback: regex scan of <a> tags when selectors miss."""
    out: List[Dict] = []
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I|re.S):
        href, label = m.group(1), re.sub(r"<[^>]+>", "", m.group(2)).strip()
        low = label.lower()
        if not href or not label: continue
        if any(k in low for k in ["greyhound", "race", "meeting", "park", "track"]):
            url = absolutize(href)
            out.append({"name": label, "url": url})
    # de-dup by url
    seen, uniq = set(), []
    for it in out:
        if it["url"] in seen: continue
        seen.add(it["url"]); uniq.append(it)
    return uniq

def main(out_dir="data/ladbrokes"):
    os.makedirs(out_dir, exist_ok=True)
    stamp = ts()
    json_path   = os.path.join(out_dir, f"meetings_{stamp}.json")
    html_path   = os.path.join(out_dir, f"debug_{stamp}.html")
    shot_path   = os.path.join(out_dir, f"page_{stamp}.png")

    cookie_raw = os.getenv("LADBROKES_COOKIE", "").strip()

    meetings: List[Dict] = []
    status_code = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1366, "height": 900},
            locale="en-AU",
        )

        # Optional: seed cookies from secret if provided
        if cookie_raw:
            cookies = []
            for pair in cookie_raw.split(";"):
                if "=" in pair:
                    k, v = pair.strip().split("=", 1)
                    cookies.append({
                        "name": k.strip(), "value": v.strip(),
                        "domain": ".ladbrokes.com.au", "path": "/",
                        "httpOnly": False, "secure": True,
                    })
            if cookies:
                ctx.add_cookies(cookies)

        page = ctx.new_page()
        try:
            resp = page.goto(LADBROKES_URL, wait_until="domcontentloaded", timeout=60000)
            status_code = resp.status if resp else 0

            # Try to accept cookie banners if present
            for sel in [
                'button:has-text("Accept")', 'button:has-text("I Accept")',
                '[data-testid="cookie-accept"]', 'button:has-text("Agree")'
            ]:
                try:
                    page.locator(sel).first.click(timeout=1500)
                except Exception:
                    pass

            # Give client scripts some time & scroll to trigger lazy content
            page.wait_for_timeout(2000)
            for _ in range(4):
                page.mouse.wheel(0, 1000)
                page.wait_for_timeout(500)

            # Try several likely selectors (tighten once we see real DOM)
            selectors = [
                "a[href*='/racing/greyhound-racing/']",
                "a[href*='/racing/greyhound/']",
                "a[href*='/greyhound-racing/']",
                "section a[href*='/racing/']",
            ]
            found = []
            for sel in selectors:
                try:
                    anchors = page.locator(sel)
                    count = anchors.count()
                    for i in range(min(count, 300)):
                        a = anchors.nth(i)
                        href = a.get_attribute("href") or ""
                        label = (a.inner_text(timeout=500) or "").strip()
                        if not href or not label: continue
                        low = label.lower()
                        if any(k in low for k in ["greyhound", "race", "meeting", "park", "track"]):
                            found.append({"name": label, "url": absolutize(href)})
                except PWTimeout:
                    continue
                except Exception:
                    continue

            # Fallback: regex scan of the rendered HTML
            if not found:
                html = page.content()
                found = extract_meetings_html(html)

            # Dedup by URL
            seen, uniq = set(), []
            for it in found:
                if it["url"] in seen: continue
                seen.add(it["url"]); uniq.append(it)
            meetings = uniq

            # Save artifacts
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(page.content())
            try:
                page.screenshot(path=shot_path, full_page=True)
            except Exception:
                pass

        finally:
            ctx.close()
            browser.close()

    payload = {
        "fetched_at_utc": stamp,
        "source": "playwright",
        "status_code": status_code,
        "count": len(meetings),
        "meetings": meetings,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"status={status_code} meetings={len(meetings)}")
    print(f"wrote: {json_path}")
    print(f"saved debug: {html_path}")
    print(f"saved screenshot: {shot_path}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data/ladbrokes")
    args = ap.parse_args()
    main(args.out_dir)

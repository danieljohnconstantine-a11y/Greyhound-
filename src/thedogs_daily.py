import re, pathlib
from bs4 import BeautifulSoup
from datetime import datetime
from .http_client import fetch
from .utils import utcstamp, write_json, write_text, OUT_BASE

OUT_DIR = OUT_BASE / "thedogs"

def _today_slug():
    # TheDogs uses yyyy-mm-dd in many routes
    return datetime.utcnow().strftime("%Y-%m-%d")

def parse_meetings_index(html: str):
    # Heuristic: find links that look like /racing/<track>/<date>/...
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if re.search(r"/racing/.+/\d{4}-\d{2}-\d{2}/", href):
            links.append(href)
    # de-dupe per track
    uniq = sorted(set(links))
    return [{"url": u} for u in uniq]

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = utcstamp()

    idx_url = f"https://www.thedogs.com.au/racing?date={_today_slug()}"
    status = 0
    meetings = []
    debug_html = ""
    try:
        r = fetch(idx_url)
        status = r.status_code
        debug_html = r.text
        meetings = parse_meetings_index(debug_html)
    except Exception as e:
        status = 0
        debug_html = f"ERROR: {e}\n"

    write_text(OUT_DIR / f"debug_{ts}.html", debug_html)
    write_json(OUT_DIR / f"meetings_{ts}.json", {
        "fetched_at_utc": ts,
        "source": "thedogs",
        "status_code": status,
        "count": len(meetings),
        "meetings": meetings,
    })

if __name__ == "__main__":
    main()

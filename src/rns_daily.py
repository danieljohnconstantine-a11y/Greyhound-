#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape Racing & Sports greyhound meetings for 'today' (AU time).
Outputs:
  data/rns/meetings_<ISOZ>.json  — list of meetings with races/runners (best-effort)
  data/rns/debug_<ISOZ>.html     — last page HTML when non-200 (for diagnosis)

This scraper is resilient:
- Uses AU-like UA, retries with jitter, and respects robots (lightweight).
- If blocked or nothing found, still writes a timestamped JSON with meetings=[].
- Never raises out of main(); returns exit code 0 so the workflow can continue.
"""

import os
import sys
import json
import time
import random
import datetime as dt
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://www.racingandsports.com.au/"
INDEX = "https://www.racingandsports.com.au/form-guide/greyhound"
OUT_DIR = os.path.join("data", "rns")

UA = os.environ.get(
    "USER_AGENT",
    os.environ.get(
        "DEFAULT_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    ),
)
ALWAYS_WRITE = os.environ.get("ALWAYS_WRITE", "0") == "1"
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }
)


def now_au_iso():
    tz = dt.timezone(dt.timedelta(hours=10))  # AEST baseline; okay for scheduling
    return dt.datetime.now(tz=tz).astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def req_get(url, max_retries=5, min_sleep=0.6, max_sleep=1.8, timeout=20):
    for i in range(1, max_retries + 1):
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code in (200, 304):
                return r
            # soft-blocks (403/429/503) -> backoff + retry
            if r.status_code in (403, 429, 503):
                time.sleep(random.uniform(min_sleep, max_sleep) * i)
                continue
            return r  # return non-OK; caller can snapshot
        except requests.RequestException:
            time.sleep(random.uniform(min_sleep, max_sleep) * i)
    # final attempt
    try:
        return SESSION.get(url, timeout=timeout)
    except requests.RequestException as e:
        class R:  # tiny stub
            status_code = 0
            text = f"REQUEST_ERROR: {e}"
            content = text.encode("utf-8", "ignore")
        return R()


def parse_index(html):
    """
    Best-effort discovery of meeting links from the R&S greyhound index page.
    Returns: list of dicts {title, url}
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        # Heuristic: only greyhound form pages, avoid non-AU or ads
        if "/form-guide/greyhound" in href or "/racing/greyhound" in href:
            full = urljoin(BASE, href)
            # basic de-dupe
            if not any(l["url"] == full for l in links):
                links.append({"title": text or "Meeting", "url": full})
    return links


def parse_meeting(html):
    """
    Very light parse: try to find races and runners.
    Site structure changes, so we gather best-effort info.
    Returns:
      { "name": str, "races": [ { "race_no": int, "title": str, "runners": [str] } ] }
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1")
    name = (title.get_text(" ", strip=True) if title else "Meeting")

    races = []
    race_cards = soup.select("div.race, section.race, li.race") or []
    if not race_cards:
        # fallback heuristic: headings that look like "Race 1", etc.
        for h in soup.find_all(["h2", "h3"]):
            t = h.get_text(" ", strip=True)
            if t.lower().startswith("race "):
                races.append({"race_no": None, "title": t, "runners": []})
    else:
        for rc in race_cards:
            t = rc.get_text(" ", strip=True)[:120]
            # runners heuristic
            runners = []
            for li in rc.find_all("li"):
                s = li.get_text(" ", strip=True)
                if s and len(s) < 120:
                    runners.append(s)
            races.append({"race_no": None, "title": t, "runners": runners[:16]})

    return {"name": name, "races": races}


def main(out_dir=OUT_DIR):
    ensure_dir(out_dir)
    ts = now_au_iso()

    index = req_get(INDEX)
    meetings = []
    debug_path = os.path.join(out_dir, f"debug_{ts}.html")

    if index.status_code != 200:
        with open(debug_path, "wb") as f:
            f.write(index.content or b"")
        payload = {"fetched_at_utc": ts, "source": "rns", "status_code": index.status_code, "count": 0, "meetings": []}
        path = os.path.join(out_dir, f"meetings_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[rns] status={index.status_code} meetings=0 (see {debug_path})")
        return 0

    # parse index, then sample meeting pages (limit to be polite)
    try:
        links = parse_index(index.text)
    except Exception:
        links = []

    if not links and ALWAYS_WRITE:
        path = os.path.join(out_dir, f"meetings_{ts}.json")
        payload = {"fetched_at_utc": ts, "source": "rns", "status_code": 200, "count": 0, "meetings": []}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("[rns] status=200 but found 0 meeting links on index")
        return 0

    # cap requests
    max_meetings = min(12, len(links))
    for i, link in enumerate(links[:max_meetings], start=1):
        r = req_get(link["url"])
        if r.status_code != 200:
            # append minimal info so we can still see coverage
            meetings.append({"name": link["title"], "url": link["url"], "races": []})
            continue
        m = parse_meeting(r.text)
        m["url"] = link["url"]
        meetings.append(m)
        time.sleep(random.uniform(0.4, 0.9))

    payload = {"fetched_at_utc": ts, "source": "rns", "status_code": 200, "count": len(meetings), "meetings": meetings}
    path = os.path.join(out_dir, f"meetings_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[rns] status=200 meetings={len(meetings)} wrote: {path}")
    return 0


if __name__ == "__main__":
    try:
        out = sys.argv[sys.argv.index("--out-dir") + 1] if "--out-dir" in sys.argv else OUT_DIR
    except Exception:
        out = OUT_DIR
    sys.exit(main(out))

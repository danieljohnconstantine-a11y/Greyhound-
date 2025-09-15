#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Racing & Sports HTML form pages for AU greyhound meetings and parse:
  track, date, race, box, runner

Enhancements:
- Saves index HTML and each meeting HTML under data/html/debug/YYYY-MM-DD/
- Clear logging: meetings_found, pages_fetched, races_found, runners_found
- Stricter meeting link filter (today) + optional ONE_MEETING_URL override
- More permissive runner detection (handles "1.", "1 -", "1 " names)
"""

from __future__ import annotations
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, List, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://www.racingandsports.com.au"
INDEX_AU = f"{BASE}/form-guide/greyhound/australia"
INDEX_ALL = f"{BASE}/form-guide/greyhound"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
HTML_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": BASE,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# Race header & runner lines
RACE_HDR_RE = re.compile(r"\b(?:RACE|Race)\s*(\d{1,2})\b")
# Accepts "Box 1 Dog", "1. Dog", "1 - Dog", "1 Dog"
BOX_LINE_RE = re.compile(r"^(?:Box\s*)?([1-8])[\s\.\-:–]+([A-Za-z0-9'().\- ]{2,})$")
BOX_FALLBACK_RE = re.compile(r"^([1-8])\s+([A-Za-z0-9'().\- ]{2,})$")

# Track code heuristic (fallback if we can’t resolve a 4–6 char upper token)
TRACK_CODE_RE = re.compile(r"\b([A-Z]{3,6})\b")

# AU date label
AUS_TZ = timezone(timedelta(hours=10))

class FetchError(Exception):
    pass

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    retry=retry_if_exception_type(FetchError),
)
def _get(url: str) -> requests.Response:
    try:
        resp = requests.get(url, headers=HTML_HEADERS, timeout=30)
    except requests.RequestException as e:
        raise FetchError(str(e)) from e
    if resp.status_code in (403, 429, 502, 503):
        raise FetchError(f"status {resp.status_code} for {url}")
    resp.raise_for_status()
    return resp

def _today_str() -> str:
    return datetime.now(AUS_TZ).strftime("%Y-%m-%d")

def _debug_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def _save_index_html(url: str, debug_dir: Path) -> str:
    r = _get(url)
    _debug_write(debug_dir / f"index_{abs(hash(url))}.html", r.text)
    return r.text

def _extract_meeting_links(index_html: str) -> List[str]:
    soup = BeautifulSoup(index_html, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        # Filter: meeting pages usually under /form-guide/greyhound/<slug>/...
        if not href.startswith("/"):
            continue
        if "/form-guide/greyhound" not in href:
            continue
        # Avoid obvious non-meeting sections (runners, tips, news)
        if any(bad in href for bad in ("/runners", "/news", "/tips", "/results")):
            continue
        out.append(BASE + href)
    # dedupe preserve order
    seen = set(); links = []
    for u in out:
        if u not in seen:
            seen.add(u); links.append(u)
    return links

def _extract_all_text(soup: BeautifulSoup) -> List[str]:
    texts: List[str] = []
    for node in soup.find_all(string=True):
        s = " ".join((node or "").strip().split())
        if s:
            texts.append(s)
    return texts

def _parse_meeting(url: str, debug_dir: Path) -> pd.DataFrame:
    r = _get(url)
    html = r.text
    _debug_write(debug_dir / f"meeting_{abs(hash(url))}.html", html)

    soup = BeautifulSoup(html, "lxml")
    all_txt = "\n".join(_extract_all_text(soup))

    # Track inference
    code = "UNKN"
    # Try breadcrumb or title first
    head = " ".join(soup.title.get_text(" ").split()) if soup.title else ""
    m = TRACK_CODE_RE.search(head) or TRACK_CODE_RE.search(url.upper()) or TRACK_CODE_RE.search(all_txt)
    if m:
        code = m.group(1)

    date_str = _today_str()
    rows = []
    current_race = None
    races_found = 0
    runners_found = 0

    for raw in all_txt.splitlines():
        line = raw.strip()
        if not line:
            continue
        hr = RACE_HDR_RE.search(line)
        if hr:
            try:
                current_race = int(hr.group(1))
                races_found += 1
            except Exception:
                current_race = None
            continue
        if current_race is None:
            continue
        mb = BOX_LINE_RE.match(line) or BOX_FALLBACK_RE.match(line)
        if mb:
            try:
                box = int(mb.group(1))
                name = mb.group(2).strip(" -–•.")
                name = re.sub(r"\s*\(.*$", "", name).strip()
                if name:
                    rows.append({"track": code, "date": date_str, "race": current_race, "box": box, "runner": name})
                    runners_found += 1
            except Exception:
                pass

    df = pd.DataFrame(rows, columns=["track","date","race","box","runner"])
    if not df.empty:
        df = (df.drop_duplicates(["track","date","race","box"])
                .sort_values(["track","race","box"])
                .reset_index(drop=True))
    print(f"[meeting] {url} races={races_found} runners={runners_found} rows={len(df)} track={code}")
    return df

def fetch_and_parse_all(debug_root: Path | None = None) -> pd.DataFrame:
    debug_root = debug_root or Path("data/html/debug") / _today_str()
    debug_root.mkdir(parents=True, exist_ok=True)

    # Optional: test a single meeting directly via env override
    override = os.environ.get("ONE_MEETING_URL", "").strip()
    links: List[str] = []
    if override:
        links = [override]
        print(f"[discover] ONE_MEETING_URL override used")
        _save_index_html(INDEX_AU, debug_root)  # still save what index looked like
    else:
        # Save both indexes for transparency
        au_html = _save_index_html(INDEX_AU, debug_root)
        all_html = _save_index_html(INDEX_ALL, debug_root)
        links = _extract_meeting_links(au_html) + _extract_meeting_links(all_html)
        # de-dup
        seen = set(); links = [u for u in links if not (u in seen or seen.add(u))]
        print(f"[discover] meetings_found={len(links)}")

    parts: List[pd.DataFrame] = []
    pages_fetched = 0
    for i, url in enumerate(links):
        time.sleep(0.7)  # be polite
        try:
            df = _parse_meeting(url, debug_root)
            pages_fetched += 1
            if not df.empty:
                parts.append(df)
        except Exception as e:
            print(f"[meeting] skip {url}: {e}")

    if not parts:
        print(f"[summary] pages_fetched={pages_fetched} total_rows=0")
        return pd.DataFrame(columns=["track","date","race","box","runner"])

    out = pd.concat(parts, ignore_index=True)
    print(f"[summary] pages_fetched={pages_fetched} total_rows={len(out)}")
    return out

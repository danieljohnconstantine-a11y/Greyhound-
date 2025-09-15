#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
html_fetch.py
Scrape Racing & Sports HTML form pages for AU greyhound meetings and parse:
  track, date, race, box, runner

Outputs are returned as a pandas.DataFrame; callers write files.

Notes
-----
- We target public HTML pages (no auth), with polite headers and small delays.
- We don't scrape odds; this is pure field/form extraction.
- Track/date are inferred from page context/URL; race/box/runner from per-race blocks.
- The site layout can vary by meeting; we use flexible parsing:
    * detect race headers like "Race 1", "RACE 2" etc
    * within each race, find "Box 1", "1.", "1 –", etc, and the runner name
- Saves optional debug HTML under data/html/debug/YYYYMMDD/
"""

from __future__ import annotations
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
INDEX_ALL = f"{BASE}/form-guide/greyhound"  # backup index

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
}

# Race/runner detection
RACE_HDR_RE = re.compile(r"\b(?:RACE|Race)\s*(\d{1,2})\b")
BOX_LINE_RE = re.compile(
    r"^(?:Box\s*)?([1-8])[\s\.\-:–]+([A-Za-z0-9'().\- ]{2,})$"
)
BOX_FALLBACK_RE = re.compile(
    r"^([1-8])\s+([A-Za-z0-9'().\- ]{2,})$"
)

# Common AU 4-letter track codes seen on PDFs (used when we need to infer)
TRACK_CODE_RE = re.compile(r"\b([A-Z]{3,6})\b")

# AEST/Brisbane-ish local date (we just need YYYY-MM-DD label)
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


def _meeting_links(index_url: str) -> List[str]:
    r = _get(index_url)
    soup = BeautifulSoup(r.text, "lxml")

    links = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        # meeting pages look like /form-guide/greyhound/<track-or-slug>/...
        if "/form-guide/greyhound" in href and href.startswith("/"):
            full = BASE + href
            links.append(full)

    # de-dup while preserving order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _debug_save(html: str, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{name}.html").write_text(html, encoding="utf-8")


def _extract_text_blocks(soup: BeautifulSoup) -> List[str]:
    """
    Pulls visible text blocks for robust regex scanning.
    """
    texts: List[str] = []
    for node in soup.find_all(text=True):
        s = " ".join((node or "").strip().split())
        if s:
            texts.append(s)
    return texts


def _parse_meeting(url: str, debug_dir: Path) -> pd.DataFrame:
    """
    Parse one meeting page into rows: track, date, race, box, runner.
    """
    resp = _get(url)
    html = resp.text
    _debug_save(html, debug_dir, f"meeting_{abs(hash(url))}")

    soup = BeautifulSoup(html, "lxml")

    # Infer track code and date
    # Try from title or breadcrumbs
    title = (soup.title.string if soup.title else "") or ""
    all_txt = " ".join(_extract_text_blocks(soup))
    # Track
    code = None
    m = TRACK_CODE_RE.search(all_txt)
    if m:
        code = m.group(1)
    elif "greyhound" in url:
        # last part of URL may contain track name, fallback to upper letters
        tail = url.rstrip("/").split("/")[-1]
        m2 = TRACK_CODE_RE.search(tail.upper())
        code = m2.group(1) if m2 else "UNKN"
    else:
        code = "UNKN"

    date_str = _today_str()  # label as today's AU date for output

    # Walk text, segment by race headers, then parse dog lines
    rows = []
    current_race = None

    lines = all_txt.splitlines()
    for raw in lines:
        line = " ".join(raw.strip().split())
        if not line:
            continue

        hr = RACE_HDR_RE.search(line)
        if hr:
            try:
                current_race = int(hr.group(1))
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
                    rows.append(
                        {"track": code, "date": date_str, "race": current_race, "box": box, "runner": name}
                    )
            except Exception:
                pass

    df = pd.DataFrame(rows, columns=["track", "date", "race", "box", "runner"])
    if not df.empty:
        df = (
            df.drop_duplicates(["track", "date", "race", "box"])
            .sort_values(["track", "race", "box"])
            .reset_index(drop=True)
        )
    return df


def fetch_and_parse_all(debug_root: Path | None = None) -> pd.DataFrame:
    """
    Returns a concatenated dataframe across all meeting pages found on R&S today.
    """
    idx_links = []
    # AU index first (preferred), then general index as backup
    for idx in (INDEX_AU, INDEX_ALL):
        try:
            lks = _meeting_links(idx)
            if lks:
                idx_links.extend(lks)
        except Exception:
            continue
        time.sleep(0.8)

    # de-dup
    seen = set()
    links = [u for u in idx_links if not (u in seen or seen.add(u))]

    if not links:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner"])

    parts: List[pd.DataFrame] = []
    for i, url in enumerate(links):
        time.sleep(0.8)
        try:
            df = _parse_meeting(url, (debug_root or Path("data/html/debug")) / _today_str())
            if not df.empty:
                parts.append(df)
        except Exception:
            continue

    if not parts:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner"])
    return pd.concat(parts, ignore_index=True)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape Racing & Sports HTML form pages for AU greyhound meetings and parse:
  track, date, race, box, runner

Reliability features:
- Multi-source discovery:
    1) Today's AU index
    2) General greyhound index
    3) Runners index (lists meetings with runners)
    4) Optional operator-maintained seed list: data/html/seed_urls.txt
- ONE_MEETING_URL env override for quick testing
- Hybrid parsing:
    * Regex from visible text (Race N, box+name lines)
    * DOM selectors for common runner tables/cards
- Debug HTML snapshots under data/html/debug/YYYY-MM-DD/
- Clear counters: meetings_found, pages_fetched, races_found, runners_found
"""

from __future__ import annotations
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://www.racingandsports.com.au"
INDEX_AU   = f"{BASE}/form-guide/greyhound/australia"
INDEX_ALL  = f"{BASE}/form-guide/greyhound"
INDEX_RUN  = f"{BASE}/form-guide/greyhound/runners"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": BASE,
}

AUS_TZ = timezone(timedelta(hours=10))

RACE_HDR_RE      = re.compile(r"\b(?:RACE|Race)\s*(\d{1,2})\b")
BOX_LINE_RE      = re.compile(r"^(?:Box\s*)?([1-8])[\s\.\-:–]+([A-Za-z0-9'().\- ]{2,})$")
BOX_FALLBACK_RE  = re.compile(r"^([1-8])\s+([A-Za-z0-9'().\- ]{2,})$")
TRACK_CODE_RE    = re.compile(r"\b([A-Z]{3,6})\b")

class FetchError(Exception):
    pass

@retry(reraise=True, stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=1, max=6),
       retry=retry_if_exception_type(FetchError))
def _get(url: str) -> requests.Response:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        raise FetchError(str(e)) from e
    if r.status_code in (403, 429, 502, 503):
        raise FetchError(f"status {r.status_code} for {url}")
    r.raise_for_status()
    return r

def _today_str() -> str:
    return datetime.now(AUS_TZ).strftime("%Y-%m-%d")

def _debug_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def _save_html(url: str, debug_dir: Path, kind: str) -> str:
    r = _get(url)
    _debug_write(debug_dir / f"{kind}_{abs(hash(url))}.html", r.text)
    return r.text

def _extract_meeting_links_from_index(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href.startswith("/"):
            continue
        # Meeting-ish pages
        if "/form-guide/greyhound" not in href:
            continue
        # Avoid non-meeting sections
        if any(bad in href for bad in ("/runners", "/news", "/tips", "/results", "/trainer", "/dog")):
            continue
        links.append(BASE + href)
    # de-dup
    seen = set(); out = []
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def _extract_meeting_links_from_runners(html: str) -> List[str]:
    """
    The runners page may list meetings or link to them; pull those.
    """
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href.startswith("/"):
            continue
        if "/form-guide/greyhound" not in href:
            continue
        if any(bad in href for bad in ("/news", "/tips", "/results")):
            continue
        links.append(BASE + href)
    seen = set(); out = []
    for u in links:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def _read_seed_urls(seed_file: Path) -> List[str]:
    if not seed_file.exists():
        return []
    urls = []
    for line in seed_file.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.startswith("http"):
            urls.append(s)
    return urls

def _extract_all_text(soup: BeautifulSoup) -> List[str]:
    texts: List[str] = []
    for node in soup.find_all(string=True):
        s = " ".join((node or "").strip().split())
        if s:
            texts.append(s)
    return texts

def _parse_dom_runners(soup: BeautifulSoup) -> List[Tuple[int,str]]:
    """
    DOM-driven extraction for robustness:
    - rows in tables where first cell is 1..8
    - list items/cards with a “box number + name”
    """
    out: List[Tuple[int,str]] = []

    # Table pattern: first column box, second column dog name
    for table in soup.select("table"):
        for tr in table.select("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            raw_box = " ".join(tds[0].get_text(" ").split())
            raw_name = " ".join(tds[1].get_text(" ").split())
            if re.fullmatch(r"[1-8]", raw_box or "") and raw_name:
                out.append((int(raw_box), raw_name))

    # Card/list pattern: elements with a number + name
    for li in soup.select("li, div"):
        txt = " ".join(li.get_text(" ").split())
        m = BOX_LINE_RE.match(txt) or BOX_FALLBACK_RE.match(txt)
        if m:
            box = int(m.group(1))
            name = m.group(2).strip(" -–•.")
            if 1 <= box <= 8 and len(name) >= 2:
                out.append((box, name))

    # de-dup by (box, name) with first wins
    seen = set(); dedup = []
    for b,n in out:
        key = (b, n.lower())
        if key in seen:
            continue
        seen.add(key)
        dedup.append((b,n))
    return dedup

def _parse_meeting(url: str, debug_dir: Path) -> pd.DataFrame:
    r = _get(url)
    html = r.text
    _debug_write(debug_dir / f"meeting_{abs(hash(url))}.html", html)

    soup = BeautifulSoup(html, "lxml")
    all_txt = "\n".join(_extract_all_text(soup))

    # Track code inference
    code = "UNKN"
    title = " ".join((soup.title.get_text(" ") if soup.title else "").split())
    m = TRACK_CODE_RE.search(title) or TRACK_CODE_RE.search(url.upper()) or TRACK_CODE_RE.search(all_txt)
    if m:
        code = m.group(1)

    date_str = _today_str()
    rows = []
    current_race = None
    races_found = 0
    runners_found = 0

    # 1) DOM-first extraction per race block (if structure exists)
    # Heuristic: split on headings that look like "Race N"
    blocks: List[Tuple[int, BeautifulSoup]] = []
    for h in soup.find_all(["h1","h2","h3","h4","h5","h6","strong","b"]):
        t = " ".join(h.get_text(" ").split())
        mr = RACE_HDR_RE.search(t)
        if mr:
            try:
                rn = int(mr.group(1))
            except Exception:
                continue
            # race content likely lives near siblings that follow this header
            # capture a window of elements after the header
            content = []
            sib = h.next_sibling
            hops = 0
            while sib is not None and hops < 50:
                if getattr(sib, "name", None) in ("h1","h2","h3","h4","h5","h6","strong","b"):
                    break
                content.append(sib)
                sib = sib.next_sibling
                hops += 1
            tmp = BeautifulSoup("".join(str(x) for x in content), "lxml")
            blocks.append((rn, tmp))

    if blocks:
        for rn, blk in blocks:
            dom_pairs = _parse_dom_runners(blk)
            if dom_pairs:
                races_found += 1
                for b, nm in dom_pairs:
                    nm = re.sub(r"\s*\(.*$", "", nm).strip()
                    rows.append({"track": code, "date": date_str, "race": rn, "box": b, "runner": nm})
                    runners_found += 1

    # 2) Fallback: regex across all visible text (catches “1. NAME” patterns)
    if not rows:
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
    print(f"[meeting] races={races_found} runners={runners_found} rows={len(df)} track={code} url={url}")
    return df

def fetch_and_parse_all(debug_root: Path | None = None) -> pd.DataFrame:
    debug_root = debug_root or Path("data/html/debug") / _today_str()
    debug_root.mkdir(parents=True, exist_ok=True)

    # Optional single-meeting override
    override = os.environ.get("ONE_MEETING_URL", "").strip()
    links: List[str] = []

    if override:
        links = [override]
        print("[discover] using ONE_MEETING_URL override")
        _save_html(INDEX_AU,   debug_root, "index")
        _save_html(INDEX_ALL,  debug_root, "index")
        _save_html(INDEX_RUN,  debug_root, "index")
    else:
        au_html  = _save_html(INDEX_AU,  debug_root, "index")
        all_html = _save_html(INDEX_ALL, debug_root, "index")
        run_html = _save_html(INDEX_RUN, debug_root, "index")

        links += _extract_meeting_links_from_index(au_html)
        links += _extract_meeting_links_from_index(all_html)
        links += _extract_meeting_links_from_runners(run_html)

        # seed list last (operator maintained)
        seed_urls = _read_seed_urls(Path("data/html/seed_urls.txt"))
        links += seed_urls

        # de-dup
        seen = set(); links = [u for u in links if not (u in seen or seen.add(u))]

    print(f"[discover] meetings_found={len(links)}")
    if not links:
        return pd.DataFrame(columns=["track","date","race","box","runner"])

    parts: List[pd.DataFrame] = []
    pages_fetched = 0
    for url in links:
        time.sleep(0.6)  # polite
        try:
            df = _parse_meeting(url, debug_root)
            pages_fetched += 1
            if not df.empty:
                parts.append(df)
        except Exception as e:
            print(f"[meeting] skip url={url}: {e}")

    if not parts:
        print(f"[summary] pages_fetched={pages_fetched} total_rows=0")
        return pd.DataFrame(columns=["track","date","race","box","runner"])

    out = pd.concat(parts, ignore_index=True)
    print(f"[summary] pages_fetched={pages_fetched} total_rows={len(out)}")
    return out

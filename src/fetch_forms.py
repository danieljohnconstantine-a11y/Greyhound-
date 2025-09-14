#!/usr/bin/env python3
"""
fetch_forms.py
Fetch AU greyhound race-form PDFs from Racing & Sports and save into ./forms.

Strategy (no inserts; self-contained):
- Crawl the R&S greyhound form page and per-meeting pages.
- Extract links that look like official "newformpdf" race-form PDFs.
- Filter out sponsor/club PDFs and anything not matching expected patterns.
- Normalise filenames to CODE_YYYY-MM-DD.pdf (e.g., RICH_2025-09-07.pdf).
- Skip already-downloaded files; retry politely; user-agent set.

This script only writes valid PDFs. If nothing valid found, it returns 0.
"""

from __future__ import annotations
import re
import time
import sys
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ----------------------------
# Config
# ----------------------------
BASE = "https://www.racingandsports.com.au"
INDEX = f"{BASE}/form-guide/greyhound"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

PDF_HEADERS = {
    **HEADERS,
    "Accept": "application/pdf,*/*;q=0.8",
    "Referer": INDEX,
}

# Reject obvious non-form PDFs (sponsors, clubs, promotional)
REJECT_PATTERNS = [
    r"ladbrokes", r"club", r"workies", r"broken[-_ ]?hill",
    r"rural[-_ ]?supplies", r"mult[iy]-?0-?2", r"odds-?surge",
    r"million[-_ ]?dollar", r"steel", r"sprint", r"top[-_ ]?dog",
]
REJECT_RE = re.compile("|".join(REJECT_PATTERNS), re.I)

# Track code detection: look for 4 uppercase letters near the link text or URL.
TRACK_CODE_RE = re.compile(r"\b([A-Z]{4})\b")

# Extract ddmm or yyyymmdd inside URL to derive date
DATE_GUESS_RE = re.compile(r"(?:(20\d{2})[-/]?(\d{2})[-/]?(\d{2}))|(?:(\d{2})(\d{2}))")


class FetchError(Exception):
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(FetchError),
)
def _get(url: str, is_pdf: bool = False) -> requests.Response:
    try:
        resp = requests.get(url, headers=PDF_HEADERS if is_pdf else HEADERS, timeout=30)
    except requests.RequestException as e:
        raise FetchError(str(e)) from e

    # R&S occasionally returns 403/429; turn that into a retry
    if resp.status_code in (403, 429, 502, 503):
        raise FetchError(f"status {resp.status_code} for {url}")

    resp.raise_for_status()
    return resp


def _meetings_from_index() -> list[str]:
    """Return meeting page URLs from the greyhound form index."""
    r = _get(INDEX)
    soup = BeautifulSoup(r.text, "lxml")

    links = []
    for a in soup.select("a[href]"):
        href = a["href"]
        # Typical meeting pages look like /racing/form-guide/greyhound/<slug> or similar
        if "/form-guide/greyhound" in href and href.startswith("/"):
            links.append(BASE + href)
    # De-dup & keep order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _pdfs_from_meeting(meeting_url: str) -> list[tuple[str, str]]:
    """
    Return list of (pdf_url, context_text) from a meeting page.
    """
    r = _get(meeting_url)
    soup = BeautifulSoup(r.text, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a["href"]
        text = " ".join(a.get_text(" ").split())
        # R&S form PDFs live under .../raceinfo/newformpdf/...
        if "newformpdf" in href and href.endswith(".pdf"):
            url = href if href.startswith("http") else (BASE + href if href.startswith("/") else href)
            out.append((url, text))
    return out


def _looks_like_form(url: str, context: str) -> bool:
    name = url.split("/")[-1]
    blob = f"{name} {context}"
    if REJECT_RE.search(blob):
        return False
    # Must be a .pdf already checked; also avoid gigantic querystrings
    return True


def _infer_track_and_date(url: str, context: str, default_year: int) -> tuple[str | None, str | None]:
    """
    Try to infer (TRACK4, YYYY-MM-DD).
    We scan both the filename and any surrounding context.
    """
    blob = f"{url} {context}"
    # Track code
    m_code = TRACK_CODE_RE.search(blob)
    code = m_code.group(1) if m_code else None

    # Date
    d = None
    m = DATE_GUESS_RE.search(blob)
    if m:
        if m.group(1):  # yyyy mm dd
            y, mo, da = m.group(1), m.group(2), m.group(3)
            d = f"{int(y):04d}-{int(mo):02d}-{int(da):02d}"
        else:  # dd mm (assume current year)
            dd, mm = m.group(4), m.group(5)
            d = f"{default_year:04d}-{int(mm):02d}-{int(dd):02d}"

    return code, d


def fetch_all(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Local “today” in Australia/Sydney: used for filename normalisation when URLs omit year.
    # (We do not rely on system TZ; we pick the AU date.)
    now_utc = datetime.now(timezone.utc)
    syd = now_utc.astimezone()
    default_year = syd.year

    meetings = _meetings_from_index()
    if not meetings:
        print("[rns] no meetings found on index (site change or temporary issue)")
        return 0

    seen_pdf_urls: set[str] = set()
    saved = 0

    for meet in meetings:
        # Be nice – small delay between pages
        time.sleep(0.8)

        try:
            pdfs = _pdfs_from_meeting(meet)
        except Exception as e:
            print(f"[rns] skip meeting {meet}: {e}")
            continue

        for pdf_url, ctx in pdfs:
            if pdf_url in seen_pdf_urls:
                continue
            seen_pdf_urls.add(pdf_url)

            if not _looks_like_form(pdf_url, ctx):
                continue

            code, d = _infer_track_and_date(pdf_url, ctx, default_year)
            # If we cannot infer track or date, still try to fetch but put it under UNKNOWN_YYYY-MM-DD
            if not d:
                d = now_utc.strftime("%Y-%m-%d")
            if not code:
                code = "UNKN"

            filename = f"{code}_{d}.pdf"
            dest = out_dir / filename
            if dest.exists() and dest.stat().st_size > 10 * 1024:  # >10KB as sanity
                continue

            try:
                resp = _get(pdf_url, is_pdf=True)
            except Exception as e:
                print(f"[rns] failed {pdf_url}: {e}")
                continue

            # Minimal PDF sanity check
            content = resp.content
            if not content.startswith(b"%PDF"):
                print(f"[rns] not a pdf (magic mismatch): {pdf_url}")
                continue
            if len(content) < 12 * 1024:  # tiny -> likely not a full form
                print(f"[rns] pdf too small (<12KB), skip: {pdf_url}")
                continue

            dest.write_bytes(content)
            saved += 1
            print(f"[rns] saved {dest.name}")

    print(f"[rns] total valid forms saved: {saved}")
    return saved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", "--out-dir", dest="out_dir", default="forms", help="output directory for PDFs")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)

    try:
        count = fetch_all(out_dir)
    except Exception as e:
        print(f"[rns] fatal: {e}", file=sys.stderr)
        sys.exit(1)

    # Exit 0 even if 0 saved: upstream wrapper decides whether to fail pipeline.
    print(f"[rns] done, saved={count}")
    print("::set-output name=saved::{}".format(count))
    return 0


if __name__ == "__main__":
    sys.exit(main())

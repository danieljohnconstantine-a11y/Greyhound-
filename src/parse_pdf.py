#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse Racing & Sports greyhound form PDFs into a normalized dataframe.

Input:
  ./forms/*.pdf              (only track files like QSTR_2025-09-08.pdf)

Output (returned as pandas.DataFrame):
  columns = ["track","date","race","box","runner"]

Notes:
- We ignore any PDF that doesn't match ^[A-Z]{3,5}_YYYY-MM-DD\.pdf
- We tolerate small layout differences by scanning text lines.
- We don't try to extract odds here; this is structure-first.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Tuple

import pdfplumber
import pandas as pd


VALID_FILE_RE = re.compile(r"^(?P<track>[A-Z]{3,5})_(?P<date>\d{4}-\d{2}-\d{2})\.pdf$")

# Race header patterns commonly seen across R&S PDFs
RACE_HEADER_RE = re.compile(r"^(?:RACE|Race)\s*(?P<race>\d{1,2})\b")

# Runner line patterns:
#   "Box 1  DOG NAME ..."  or "1 DOG NAME ..." (some PDFs omit the word "Box")
BOX_LINE_RE = re.compile(
    r"^(?:Box\s*)?(?P<box>[1-8])\b[\s\-:]+(?P<name>[A-Za-z0-9'().\- ]{2,})"
)

# Fallback when the above doesn't trip but the line looks like "1  DOG NAME"
BOX_FALLBACK_RE = re.compile(
    r"^(?P<box>[1-8])\s+(?P<name>[A-Za-z0-9'().\- ]{2,})"
)


def iter_valid_pdfs(forms_dir: Path) -> Iterable[Tuple[Path, str, str]]:
    for pdf_path in sorted(forms_dir.glob("*.pdf")):
        m = VALID_FILE_RE.match(pdf_path.name)
        if not m:
            continue
        yield pdf_path, m.group("track"), m.group("date")


def extract_text_lines(pdf_path: Path) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            # Normalize Windows newlines and trim whitespace
            for raw in text.splitlines():
                line = " ".join(raw.strip().split())
                if line:
                    lines.append(line)
    return lines


def parse_pdf(pdf_path: Path, track: str, date_str: str) -> pd.DataFrame:
    """
    Return rows for one PDF: columns track, date, race, box, runner
    """
    rows: List[Tuple[str, str, int, int, str]] = []
    lines = extract_text_lines(pdf_path)

    current_race: int | None = None
    for line in lines:
        # Race header?
        mr = RACE_HEADER_RE.search(line)
        if mr:
            try:
                current_race = int(mr.group("race"))
            except Exception:
                current_race = None
            continue

        if current_race is None:
            continue  # skip until we see a race header

        # Runner lines
        mb = BOX_LINE_RE.search(line)
        if not mb:
            mb = BOX_FALLBACK_RE.search(line)

        if mb:
            try:
                box = int(mb.group("box"))
                name = mb.group("name").strip(" -–•.")
                # prune trailing trainer or bracket chunks if fused
                name = re.sub(r"\s*\(.*$", "", name).strip()
                if name:
                    rows.append((track, date_str, current_race, box, name))
            except Exception:
                pass

    df = pd.DataFrame(rows, columns=["track", "date", "race", "box", "runner"])
    # Deduplicate in case both patterns match the same line
    if not df.empty:
        df = (
            df.drop_duplicates(["track", "date", "race", "box"])
            .sort_values(["track", "date", "race", "box"])
            .reset_index(drop=True)
        )
    return df


def parse_forms(forms_dir: str | Path = "forms") -> pd.DataFrame:
    forms_dir = Path(forms_dir)
    all_parts: List[pd.DataFrame] = []

    for pdf_path, track, date_str in iter_valid_pdfs(forms_dir):
        try:
            part = parse_pdf(pdf_path, track, date_str)
            if not part.empty:
                all_parts.append(part)
        except Exception as e:
            # Don't crash the whole run for a single bad PDF; just skip.
            print(f"[parse] WARN skipping {pdf_path.name}: {e}")

    if not all_parts:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner"])

    df = pd.concat(all_parts, ignore_index=True)
    return df


if __name__ == "__main__":
    out = parse_forms("forms")
    print(out.head(20).to_string(index=False))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse Racing & Sports greyhound form PDFs into a normalized dataframe.

We only accept files named like: TRACK_YYYY-MM-DD.pdf (e.g., QSTR_2025-09-08.pdf)

Output columns: track, date, race, box, runner
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Iterable, List, Tuple
import pdfplumber
import pandas as pd

VALID_FILE_RE = re.compile(r"^(?P<track>[A-Z]{3,5})_(?P<date>\d{4}-\d{2}-\d{2})\.pdf$")
RACE_HEADER_RE = re.compile(r"^(?:RACE|Race)\s*(?P<race>\d{1,2})\b")
BOX_LINE_RE = re.compile(r"^(?:Box\s*)?(?P<box>[1-8])\b[\s\-:]+(?P<name>[A-Za-z0-9'().\- ]{2,})")
BOX_FALLBACK_RE = re.compile(r"^(?P<box>[1-8])\s+(?P<name>[A-Za-z0-9'().\- ]{2,})")

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
            for raw in text.splitlines():
                line = " ".join(raw.strip().split())
                if line:
                    lines.append(line)
    return lines

def parse_pdf(pdf_path: Path, track: str, date_str: str) -> pd.DataFrame:
    rows: List[Tuple[str, str, int, int, str]] = []
    lines = extract_text_lines(pdf_path)
    current_race: int | None = None

    for line in lines:
        mr = RACE_HEADER_RE.search(line)
        if mr:
            try:
                current_race = int(mr.group("race"))
            except Exception:
                current_race = None
            continue

        if current_race is None:
            continue

        mb = BOX_LINE_RE.search(line) or BOX_FALLBACK_RE.search(line)
        if mb:
            try:
                box = int(mb.group("box"))
                name = mb.group("name").strip(" -–•.")
                name = re.sub(r"\s*\(.*$", "", name).strip()
                if name:
                    rows.append((track, date_str, current_race, box, name))
            except Exception:
                pass

    df = pd.DataFrame(rows, columns=["track", "date", "race", "box", "runner"])
    if not df.empty:
        df = (df.drop_duplicates(["track", "date", "race", "box"])
                .sort_values(["track", "date", "race", "box"])
                .reset_index(drop=True))
    return df

def parse_forms(forms_dir: str | Path = "forms") -> pd.DataFrame:
    forms_dir = Path(forms_dir)
    parts: List[pd.DataFrame] = []
    for pdf_path, track, date_str in iter_valid_pdfs(forms_dir):
        try:
            part = parse_pdf(pdf_path, track, date_str)
            if not part.empty:
                parts.append(part)
        except Exception as e:
            print(f"[parse] WARN skipping {pdf_path.name}: {e}")
    if not parts:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner"])
    return pd.concat(parts, ignore_index=True)

if __name__ == "__main__":
    df = parse_forms("forms")
    print(df.head(40).to_string(index=False))

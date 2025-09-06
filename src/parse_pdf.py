from __future__ import annotations
from pathlib import Path
import re
import pdfplumber
import pandas as pd
from datetime import datetime
from .utils import infer_track_and_date_from_name

# Output schema
COLUMNS = ["track", "date", "race", "distance_m", "box", "runner", "trainer", "comments", "form_text"]

# --- Robust detectors ---------------------------------------------------------
# Works for "Race 5", "R5", "R 12"
RACE_MARK_RE = re.compile(r"\b(?:Race|R)\s*([1-2]?\d)\b", re.I)
# Works for "515m", "350 m"
DIST_MARK_RE = re.compile(r"\b(\d{3,4})\s*m\b", re.I)

# Accepts:
# "1 Runner Name", "Box 1 Runner Name", "1) Runner Name", "1. Runner Name"
BOX_RUNNER_RE = re.compile(
    r"^\s*(?:Box\s*)?([1-8])[\)\.\-:]?\s+([A-Za-z][A-Za-z0-9 '\-\.&/]+?)\s{2,}.*$|^\s*(?:Box\s*)?([1-8])[\)\.\-:]?\s+([A-Za-z][A-Za-z0-9 '\-\.&/]+?)\s*$"
)

# Obvious noise (percent strips, weights, times, headings)
NOISE_RE = re.compile(r"(?:\b\d+%-\d+%|\b\d+%\b|\b\d+\.?\d*kg\b|\bHorse:|\bTote:|\bSP:|\bTime:|\bSectional)", re.I)

MULTISPACE_RE = re.compile(r"\s{2,}")

def _clean_runner(s: str) -> str:
    s = s.strip(" -•·—\t")
    s = MULTISPACE_RE.sub(" ", s)
    return s.strip()

def _write_debug_text(pdf_path: Path, all_text: str, out_dir: Path) -> None:
    """Write raw page text to help adjust regex if needed."""
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        dbg = out_dir / f"{pdf_path.stem}.txt"
        dbg.write_text(all_text, encoding="utf-8")
    except Exception:
        pass  # never fail the run for debug writes

def parse_form_pdf(pdf_path: Path, debug_out: Path | None = None) -> pd.DataFrame:
    track, dt = infer_track_and_date_from_name(pdf_path)
    rows = []
    race = None
    dist_m = None

    all_text_accum = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            all_text_accum.append(text)

            # detect race/distance anywhere on the page
            mr = RACE_MARK_RE.search(text)
            if mr:
                try:
                    race = int(mr.group(1))
                except Exception:
                    pass
            md = DIST_MARK_RE.search(text)
            if md:
                try:
                    dist_m = int(md.group(1))
                except Exception:
                    pass

            for raw in text.splitlines():
                line = raw.strip()
                if not line or NOISE_RE.search(line):
                    continue

                # also allow race markers inline on lines
                mr2 = RACE_MARK_RE.search(line)
                if mr2:
                    try:
                        race = int(mr2.group(1))
                    except Exception:
                        pass

                m = BOX_RUNNER_RE.match(line)
                if not m:
                    continue

                box = m.group(1) or m.group(3)
                name = m.group(2) or m.group(4)
                try:
                    box = int(box)
                except Exception:
                    continue

                runner = _clean_runner(name)
                if not runner or len(runner) < 2:
                    continue
                if "%" in runner or "kg" in runner.lower():
                    continue
                if race is None:
                    continue

                rows.append([track, dt, race, dist_m, box, runner, None, raw, None])

    if debug_out is not None:
        _write_debug_text(pdf_path, "\n\n=== PAGE SPLIT ===\n\n".join(all_text_accum), debug_out)

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    out = pd.DataFrame(rows, columns=COLUMNS)
    out.sort_values(["race", "box"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out

from __future__ import annotations
from pathlib import Path
import re
import pdfplumber
import pandas as pd
from .utils import RACE_RE, BOX_RE, DIST_RE, infer_track_and_date_from_name

# Final output schema
COLUMNS = ["track", "date", "race", "distance_m", "box", "runner", "trainer", "comments", "form_text"]

# Skip lines that are clearly not runner rows
NOISE_PATTERNS = (
    r"\b\d+\.?\d*kg\b",         # weights like "0kg" / "32.5kg"
    r"\bHorse:\b",              # horse racing artefacts
    r"\b\d{1,3}\.\d{2}\b",      # times like 24.16
    r"\b\d+%-\d+%\b",           # "0%-100%"
    r"\b\d+%\b",                # single percentages
)

NOISE_RE = re.compile("|".join(NOISE_PATTERNS), re.I)

# Strict box+runner capture:
#   "1  Runner Name" or "Box 1 Runner Name" or "1. Runner Name" or "1) Runner Name"
#   Capture runner as words/symbols, stop before two+ spaces followed by digits/symbols (odds/times)
BOX_RUNNER_RE = re.compile(
    r"^(?:\s*(?:Box\s*)?([1-8])[\)\.\-:]?)\s+([A-Za-z][A-Za-z0-9 '\-\.&/]+?)"
    r"(?:\s{2,}.*)?$"
)

# Tidy runner names
BAD_TRAIL_RE = re.compile(
    r"\s+(?:(?:\d{1,3}\.\d{2})|(?:\(\d+\))|(?:odds?:?.*)|(?:SP:.*)|(?:Time:.*)|(?:Tote:.*))\s*$",
    re.I,
)
MULTISPACE_RE = re.compile(r"\s{2,}")


def _clean_runner(s: str) -> str:
    s = BAD_TRAIL_RE.sub("", s)
    s = s.strip(" -•·—\t")
    s = MULTISPACE_RE.sub(" ", s)
    return s.strip()


def parse_form_pdf(pdf_path: Path) -> pd.DataFrame:
    track, dt = infer_track_and_date_from_name(pdf_path)
    rows = []
    race = None
    dist_m = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            # update context for race & distance whenever seen on this page
            r = RACE_RE.search(text)
            if r:
                try:
                    race = int(r.group(1))
                except Exception:
                    pass
            d = DIST_RE.search(text)
            if d:
                try:
                    dist_m = int(d.group(1))
                except Exception:
                    pass

            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue

                # hard-filter obvious noise lines
                if NOISE_RE.search(line):
                    continue

                # must match a box+runner at the start
                m = BOX_RUNNER_RE.match(line)
                if not m:
                    continue

                try:
                    box = int(m.group(1))
                except Exception:
                    continue

                runner = _clean_runner(m.group(2))
                # discard bogus runner "1", "0", percentages, or very short tokens
                if not runner or len(runner) < 2:
                    continue
                if any(ch.isdigit() for ch in runner):  # avoid stray numeric-only/percent junk
                    # keep if digits appear inside a legitimate-looking name like "Plan B" (single digit)
                    if not re.search(r"[A-Za-z]\d[A-Za-z]?", runner):
                        # but allow things like "Section 8" -> letters + space + digit allowed
                        if not re.search(r"[A-Za-z]+\s+\d", runner):
                            continue
                if "%" in runner or "kg" in runner.lower() or "horse:" in runner.lower():
                    continue

                rows.append([track, dt, race, dist_m, box, runner, None, raw_line.strip(), None])

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    out = pd.DataFrame(rows, columns=COLUMNS)

    # Final clean & types
    out = out.dropna(subset=["box", "runner"], how="any")
    # Keep only races we identified (avoid NaN races causing grouping issues)
    out = out[out["race"].notna()]
    if out.empty:
        return pd.DataFrame(columns=COLUMNS)

    out["race"] = out["race"].astype(int, errors="ignore")
    out["box"] = out["box"].astype(int, errors="ignore")
    out.sort_values(["race", "box"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out

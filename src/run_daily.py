# src/run_daily.py
"""
Daily pipeline:
- read PDFs in forms/ (optionally filtered to a target date)
- extract raw text (pdfminer.six)
- parse runners with src.parse_pdf.parse_form_pdf(...)
- group runners into races using box order (1..8)
- build simple features & a naive probability model
- write reports/{YYYY-MM-DD}/probabilities.csv and summary.md
- save raw text to reports/{date}/debug/<PDF>.txt for regex tuning

Run locally or in GitHub Actions:
  python -m src.run_daily --forms-dir forms --date today
  python -m src.run_daily --forms-dir forms --date 2025-09-06
"""

from __future__ import annotations
import argparse
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from pdfminer.high_level import extract_text

# our parser (you already created this)
from .parse_pdf import parse_form_pdf


# -----------------------------
# Helpers
# -----------------------------
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

def today_yyyymmdd() -> str:
    # Use UTC to match GitHub Actions runner date; adjust if you prefer AU time
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_target_date(date_arg: str) -> str:
    if date_arg.lower() == "today":
        return today_yyyymmdd()
    m = DATE_RE.search(date_arg)
    if not m:
        raise SystemExit(f"--date must be 'today' or YYYY-MM-DD, got: {date_arg}")
    return m.group(1)

def pick_track_and_date_from_filename(filename: str) -> (str, str | None):
    """
    Expect names like CANN_2025-09-06.pdf or RICH_2025-09-05.pdf
    Returns (track, date_str|None)
    """
    stem = Path(filename).stem
    parts = stem.split("_")
    track = parts[0] if parts else stem
    date_match = DATE_RE.search(stem)
    return track, (date_match.group(1) if date_match else None)

def group_into_races(parsed_lines: List[Dict[str, Any]]) -> List[int]:
    """
    Assign race_id by detecting box==1 as a new race boundary.
    Works for common forms that list runners in box order per race.
    """
    race_ids = []
    race_idx = 1
    last_box = None
    for row in parsed_lines:
        box = row.get("box")
        if box == 1 and last_box is not None:
            race_idx += 1
        race_ids.append(race_idx)
        last_box = box
    return race_ids

def simple_prob_from_box(box: int) -> float:
    """
    Very naive prior based on box (inside bias).
    Tune later once you have richer features.
    """
    # Box 1 best → highest score; Box 8 lowest
    base = {1: 1.20, 2: 1.12, 3: 1.06, 4: 1.00, 5: 0.96, 6: 0.92, 7: 0.90, 8: 0.88}
    return base.get(int(box), 1.0)

# -----------------------------
# Core pipeline
# -----------------------------
def process_pdf(pdf_path: Path, report_debug_dir: Path) -> pd.DataFrame:
    """
    Extract text, parse runners, add race grouping & basic features.
    Returns a DataFrame with columns:
      track, date, race, box, runner, prior_score
    """
    raw_text = extract_text(str(pdf_path)) or ""
    track, inferred_date = pick_track_and_date_from_filename(pdf_path.name)

    parsed = parse_form_pdf(
        raw_text,
        debug_path=str(report_debug_dir),
        pdf_name=pdf_path.stem
    )

    if not parsed:
        # Return empty DF but leave a debug text file for inspection
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner", "prior_score"])

    # Assign race ids
    race_ids = group_into_races(parsed)

    rows = []
    for row, race_id in zip(parsed, race_ids):
        box = int(row["box"])
        runner = str(row["runner"]).strip()
        rows.append({
            "track": track,
            "date": inferred_date,
            "race": race_id,
            "box": box,
            "runner": runner,
            "prior_score": simple_prob_from_box(box)
        })

    return pd.DataFrame(rows)


def to_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert prior_score to probabilities within each (track, race).
    """
    if df.empty:
        return df.assign(prob=np.nan, rank=np.nan)

    out = []
    for (track, race), g in df.groupby(["track", "race"], sort=True):
        scores = g["prior_score"].replace(0, 1e-6).to_numpy(dtype=float)
        probs = scores / scores.sum()
        sub = g.copy()
        sub["prob"] = probs
        sub["rank"] = sub["prob"].rank(ascending=False, method="min").astype(int)
        out.append(sub)

    return pd.concat(out, ignore_index=True) if out else df


def write_summary(probs: pd.DataFrame, pdf_files: List[str], summary_md: Path) -> None:
    lines = []
    date_str = probs["date"].dropna().iloc[0] if not probs.empty else today_yyyymmdd()
    lines.append(f"# Summary – {date_str}")
    lines.append("")
    lines.append(f"PDFs: {pdf_files}")
    lines.append("")
    lines.append("## Top pick per race")
    lines.append("")
    if probs.empty:
        lines.append("_No runners parsed. Check `debug/` text dumps for format._")
    else:
        for (track, race), g in probs.groupby(["track", "race"], sort=True):
            top = g.sort_values("prob", ascending=False).iloc[0]
            p = f"{top['prob']:.2%}"
            lines.append(f"- **{track} R{race}** — **{top['runner']}** (Box {top['box']}, {p})")
    summary_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--forms-dir", default="forms", help="Directory with form PDFs")
    parser.add_argument("--date", default="today", help="'today' or YYYY-MM-DD")
    args = parser.parse_args()

    target_date = get_target_date(args.date)

    repo_root = Path(__file__).resolve().parents[1]  # project root
    forms_dir = (repo_root / args.forms_dir).resolve()
    if not forms_dir.exists():
        raise SystemExit(f"Forms directory not found: {forms_dir}")

    # Collect PDFs (filter by date token in filename)
    pdfs = sorted(p for p in forms_dir.glob("*.pdf") if target_date in p.name)

    # Safety: if none found for target_date, fall back to all PDFs
    if not pdfs:
        pdfs = sorted(forms_dir.glob("*.pdf"))

    report_dir = (repo_root / "reports" / target_date).resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = report_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    # Process each PDF → concatenate
    frames = []
    pdf_names = []
    for pdf in pdfs:
        try:
            df = process_pdf(pdf, debug_dir)
            if not df.empty:
                # Fill date with target if filename lacked it
                if "date" not in df or df["date"].isna().all():
                    df["date"] = target_date
                frames.append(df)
            pdf_names.append(pdf.name)
        except Exception as e:
            # Leave a note in debug and continue with other files
            (debug_dir / f"{pdf.stem}.error.txt").write_text(str(e), encoding="utf-8")

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=["track", "date", "race", "box", "runner", "prior_score"])

    probs = to_probabilities(combined)

    # Save artifacts
    probabilities_csv = report_dir / "probabilities.csv"
    summary_md = report_dir / "summary.md"
    probs.sort_values(["track", "race", "rank"], inplace=True, ignore_index=True)
    probs.to_csv(probabilities_csv, index=False)
    write_summary(probs, pdf_names, summary_md)

    print(f"Saved {probabilities_csv.relative_to(repo_root)}")
    print(f"Saved {summary_md.relative_to(repo_root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

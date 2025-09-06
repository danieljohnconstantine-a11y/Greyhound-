# src/run_daily.py
import argparse
import os
import re
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple

import pandas as pd
from pdfminer.high_level import extract_text

# our parser
from .parse_pdf import parse_form_pdf


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build daily greyhound probabilities from form PDFs.")
    p.add_argument("--forms-dir", default="forms", help="Folder containing form PDFs")
    p.add_argument(
        "--date",
        default="today",
        help='Date to process: "today" or YYYY-MM-DD (must match PDF filenames)',
    )
    return p.parse_args()


def resolve_date(date_arg: str) -> str:
    if date_arg.lower() == "today":
        # Australian eastern time (roughly; avoids rolling into previous UTC day)
        aedt = timezone(timedelta(hours=10))
        return datetime.now(aedt).strftime("%Y-%m-%d")
    return date_arg


def find_pdfs(forms_dir: str, ymd: str) -> List[str]:
    """Return absolute paths for PDFs whose filenames contain _YYYY-MM-DD.pdf."""
    if not os.path.isdir(forms_dir):
        return []
    hits = []
    suffix = f"_{ymd}.pdf"
    for name in os.listdir(forms_dir):
        if name.upper().endswith(".PDF") and suffix in name:
            hits.append(os.path.join(forms_dir, name))
    return sorted(hits)


def split_text_by_race(raw_text: str) -> List[Tuple[int, str]]:
    """
    Try to split a meeting PDF into (race_number, text_block) pairs.
    If no race headers are found, return a single block with race 1.
    """
    lines = raw_text.splitlines()
    # Regex captures "Race 5", "RACE 10", "Race 1 –", etc.
    race_hdr = re.compile(r"^\s*RACE\s*(\d+)\b", re.IGNORECASE)

    blocks: List[Tuple[int, List[str]]] = []
    current_race = None
    current_lines: List[str] = []

    for ln in lines:
        m = race_hdr.match(ln)
        if m:
            # flush previous block
            if current_race is not None and current_lines:
                blocks.append((current_race, current_lines))
            current_race = int(m.group(1))
            current_lines = []
        else:
            if current_race is None:
                # haven’t seen a header yet; keep collecting in case there are none
                current_race = 1
            current_lines.append(ln)

    if current_race is not None and current_lines:
        blocks.append((current_race, current_lines))

    if not blocks:
        # fallback: the whole text is one race
        return [(1, raw_text)]
    return [(rn, "\n".join(blines)) for rn, blines in blocks]


def process_pdf(pdf_path: str, ymd: str, debug_root: str) -> pd.DataFrame:
    """
    Extract runners from a single PDF and return a DataFrame with columns:
    [track, date, race, box, runner]
    """
    fname = os.path.basename(pdf_path)
    # Track code = leading letters before underscore (e.g., "CANN_2025-09-06.pdf")
    track = fname.split("_")[0]

    try:
        text = extract_text(pdf_path) or ""
    except Exception as e:
        print(f"[warn] Failed to read PDF {fname}: {e}")
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner"])

    pieces = split_text_by_race(text)
    rows: List[Dict] = []

    for race_no, race_text in pieces:
        # write raw per-race text for debugging
        race_debug_dir = os.path.join(debug_root, track, f"race_{race_no:02d}")
        runners = parse_form_pdf(race_text, debug_path=race_debug_dir, pdf_name=fname)

        for r in runners:
            rows.append(
                {
                    "track": track,
                    "date": ymd,
                    "race": race_no,
                    "box": r.get("box"),
                    "runner": r.get("runner"),
                }
            )

    if not rows:
        print(f"[warn] No runners parsed from: {fname} (raw text saved to {os.path.join(debug_root, track)})")

    return pd.DataFrame(rows)


def assign_equal_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign simple equal-win probabilities within each (track, date, race) group.
    This is a baseline so the pipeline always produces an output even if
    advanced features aren’t available.
    """
    if df.empty:
        df["prob_win"] = []
        return df

    df = df.copy()
    df["prob_win"] = 0.0
    group_cols = ["track", "date", "race"]

    def _set_probs(g: pd.DataFrame) -> pd.DataFrame:
        n = max(len(g), 1)
        g["prob_win"] = 1.0 / n
        return g

    return df.groupby(group_cols, as_index=False, group_keys=False).apply(_set_probs)


def write_reports(all_probs: pd.DataFrame, out_dir: str, pdf_names: List[str]) -> None:
    os.makedirs(out_dir, exist_ok=True)

    # probabilities.csv
    prob_path = os.path.join(out_dir, "probabilities.csv")
    all_probs.sort_values(["track", "race", "box"], inplace=True)
    all_probs.to_csv(prob_path, index=False)
    print(f"Saved {prob_path}")

    # summary.md
    md_path = os.path.join(out_dir, "summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Summary — {os.path.basename(out_dir)}\n\n")
        f.write(f"**PDFs processed ({len(pdf_names)}):** {pdf_names}\n\n")

        if all_probs.empty:
            f.write("_No runners parsed._\n")
        else:
            f.write("## Top pick per race (uniform baseline)\n\n")
            for (track, race), g in all_probs.groupby(["track", "race"]):
                # pick the smallest box (tie-breaker) since probs are uniform
                pick = g.sort_values(["prob_win", "box"], ascending=[False, True]).iloc[0]
                f.write(f"- **{track} R{int(race)}** → Box {int(pick['box'])} — {pick['runner']}  "
                        f"(p={pick['prob_win']:.3f})\n")
    print(f"Saved {md_path}")


def main() -> int:
    args = parse_args()
    ymd = resolve_date(args.date)

    pdfs = find_pdfs(args.forms_dir, ymd)
    if not pdfs:
        print("Parser found no PDFs for the day.")
        return 2

    reports_dir = os.path.join("reports", ymd)
    debug_dir = os.path.join("debug", ymd)

    frames: List[pd.DataFrame] = []
    for p in pdfs:
        frames.append(process_pdf(p, ymd, debug_dir))

    all_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["track", "date", "race", "box", "runner"])
    all_probs = assign_equal_probabilities(all_df)
    write_reports(all_probs, reports_dir, [os.path.basename(p) for p in pdfs])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

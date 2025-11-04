#!/usr/bin/env python3
import os
import datetime as dt
import pandas as pd
from dateutil import tz

from fetch_forms import fetch_for_date, sydney_today
from parse_pdf import parse_folder

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def uniform_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(prob_win=[])
    # assume 8 boxes; uniform baseline
    return df.assign(prob_win=1.0/8.0)

def write_reports(df: pd.DataFrame, out_root: str, date_str: str):
    today_dir = os.path.join(out_root, date_str)
    latest_dir = os.path.join(out_root, "latest")
    ensure_dirs(today_dir, latest_dir)

    if df.empty:
        with open(os.path.join(latest_dir, "summary.md"), "w") as f:
            f.write("# Summary — empty data\n")
        with open(os.path.join(today_dir, "summary.md"), "w") as f:
            f.write("# Summary — empty data\n")
        return

    # probabilities
    probs = uniform_probabilities(df)
    probs_path_today = os.path.join(today_dir, "probabilities.csv")
    probs_path_latest = os.path.join(latest_dir, "probabilities.csv")
    probs.to_csv(probs_path_today, index=False)
    probs.to_csv(probs_path_latest, index=False)

    # basic “top pick” summary (box 1 by default on uniform)
    # but show first race per track for quick glance
    lines = [f"# Summary — {date_str}", ""]
    for (t, r), g in probs.groupby(["track","race"]):
        pick = g.sort_values(["prob_win","box"], ascending=[False, True]).iloc[0]
        lines.append(f"- {t} R{int(r)} → Box {int(pick['box'])} — {pick['runner']} (p={pick['prob_win']:.3f})")

    summary = "\n".join(lines) + "\n"
    with open(os.path.join(today_dir, "summary.md"), "w") as f:
        f.write(summary)
    with open(os.path.join(latest_dir, "summary.md"), "w") as f:
        f.write(summary)

def main(out_forms: str, out_rns: str, out_reports: str, date_override: str | None):
    date_str = date_override or sydney_today()
    ensure_dirs(out_forms, out_rns, out_reports)

    fetched = fetch_for_date(date_str, out_forms)
    total = sum(len(v) for v in fetched.values())
    print(f"[run] fetched={total} pdfs on {date_str}")

    parsed = parse_folder(out_forms)
    print(f"[run] parsed_rows={len(parsed)}")
    if not parsed.empty:
        parsed_path = os.path.join(out_rns, f"parsed_{date_str}.csv")
        parsed.to_csv(parsed_path, index=False)
        print(f"[run] wrote {parsed_path}")
    else:
        print("[run] no parsed rows")

    write_reports(parsed, out_reports, date_str)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-forms", default="forms")
    ap.add_argument("--out-rns", default="data/rns")
    ap.add_argument("--out-reports", default="reports")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (AEST); default = today")
    args = ap.parse_args()

    main(args.out_forms, args.out_rns, args.out_reports, args.date)

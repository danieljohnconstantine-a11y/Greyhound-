#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_daily_html.py
- Scrape today's AU greyhound meetings from R&S HTML pages
- Parse to rows (track,date,race,box,runner)
- Save parsed CSV: data/html/parsed_<UTCSTAMP>.csv
- Build uniform probabilities per race and write:
    reports/latest/probabilities.csv
    reports/latest/summary.md
- Exit 2 if no data (so workflow fails red instead of committing empties)
"""

from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from html_fetch import fetch_and_parse_all


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_probs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner", "prob_win"])
    rows = []
    for (t, d, r), grp in df.groupby(["track", "date", "race"], sort=True):
        n = max(len(grp), 1)
        p = round(1.0 / n, 6)
        for _, g in grp.iterrows():
            rows.append(
                {
                    "track": t,
                    "date": d,
                    "race": int(r),
                    "box": int(g["box"]),
                    "runner": g["runner"],
                    "prob_win": p,
                }
            )
    out = pd.DataFrame(rows).sort_values(["track", "race", "box"]).reset_index(drop=True)
    return out


def write_summary(prob_df: pd.DataFrame, out_path: Path) -> None:
    if prob_df.empty:
        out_path.write_text("## Summary — empty data\n", encoding="utf-8")
        return

    newest_date = sorted(prob_df["date"].unique())[-1]
    lines = [f"# Summary — {newest_date}", "", "Top pick per race (uniform baseline)", ""]
    for (t, d, r), grp in prob_df.groupby(["track", "date", "race"], sort=True):
        pick = grp.sort_values(["prob_win", "box"], ascending=[False, True]).iloc[0]
        lines.append(
            f"- **{t}** R{int(r)} → Box {int(pick['box'])} — {pick['runner']} (p={pick['prob_win']:.3f})"
        )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data" / "html"
    reports_latest = repo_root / "reports" / "latest"
    ensure_dir(data_dir)
    ensure_dir(reports_latest)

    df = fetch_and_parse_all(debug_root=repo_root / "data" / "html" / "debug")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parsed_csv = data_dir / f"parsed_{ts}.csv"
    df.to_csv(parsed_csv, index=False)

    if df.empty:
        # Write minimal marker and fail to avoid green-empty commits
        (reports_latest / "summary.md").write_text("## Summary — empty data\n", encoding="utf-8")
        print("[html] No rows parsed — wrote empty summary.")
        return 2

    probs = build_probs(df)
    probs.to_csv(reports_latest / "probabilities.csv", index=False)
    write_summary(probs, reports_latest / "summary.md")

    print(f"[html] parsed_rows={len(df)}")
    print(f"[html] wrote: {parsed_csv}")
    print(f"[html] wrote: {reports_latest / 'probabilities.csv'}")
    print(f"[html] wrote: {reports_latest / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
End-to-end daily builder:
 - Reads valid PDFs from ./forms
 - Parses runners into ./data/rns/parsed_YYYYMMDD.csv
 - Builds simple probabilities (uniform per race)
 - Writes ./reports/latest/probabilities.csv and ./reports/latest/summary.md
 - Exits non-zero if no valid forms were parsed (so the workflow signals empty day)
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from parse_pdf import parse_forms


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner", "prob_win"])

    # uniform per race
    probs = []
    for (track, date_str, race), grp in df.groupby(["track", "date", "race"], sort=True):
        n = max(len(grp), 1)
        p = 1.0 / n
        for _, row in grp.iterrows():
            probs.append({
                "track": track,
                "date": date_str,
                "race": race,
                "box": int(row["box"]),
                "runner": row["runner"],
                "prob_win": round(p, 6),
            })
    out = pd.DataFrame(probs).sort_values(["track", "date", "race", "box"]).reset_index(drop=True)
    return out


def write_summary(prob_df: pd.DataFrame, out_md: Path) -> None:
    if prob_df.empty:
        out_md.write_text("## Summary — empty data\n", encoding="utf-8")
        return

    lines = []
    newest_date = sorted(prob_df["date"].unique())[-1]
    lines.append(f"# Summary — {newest_date}\n")
    lines.append("Top pick per race (uniform baseline)\n")

    for (track, date_str, race), grp in prob_df.groupby(["track", "date", "race"], sort=True):
        # same prob for all; pick smallest box as tie-break
        pick = grp.sort_values(["prob_win", "box"], ascending=[False, True]).iloc[0]
        lines.append(f"- **{track}** R{race} → Box {int(pick['box'])} — {pick['runner']} (p={pick['prob_win']:.3f})")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    forms_dir = repo_root / "forms"
    data_rns_dir = repo_root / "data" / "rns"
    reports_dir = repo_root / "reports"
    latest_dir = reports_dir / "latest"

    ensure_dir(data_rns_dir)
    ensure_dir(latest_dir)

    df = parse_forms(forms_dir)
    # Save raw parsed CSV with today's stamp (UTC)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    parsed_csv = data_rns_dir / f"parsed_{stamp}.csv"
    df.to_csv(parsed_csv, index=False)

    if df.empty:
        # also update summary as "empty" so UI shows *something*
        write_summary(pd.DataFrame(), latest_dir / "summary.md")
        # non-zero so the workflow displays attention
        print("[run_daily] No valid forms parsed — wrote empty summary.")
        return 2

    probs = build_probabilities(df)
    probs_csv = latest_dir / "probabilities.csv"
    probs.to_csv(probs_csv, index=False)

    # human summary
    write_summary(probs, latest_dir / "summary.md")

    print(f"[run_daily] Parsed rows: {len(df)}")
    print(f"[run_daily] Wrote: {parsed_csv}")
    print(f"[run_daily] Wrote: {probs_csv}")
    print(f"[run_daily] Wrote: {latest_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

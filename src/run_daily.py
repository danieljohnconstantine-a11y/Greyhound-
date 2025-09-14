#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pipeline:
  - read valid PDFs in ./forms
  - parse to ./data/rns/parsed_TIMESTAMP.csv
  - compute uniform win probabilities per race
  - OPTIONAL: if ./forms/odds.csv exists, compute value bets + stakes
  - write ./reports/latest/probabilities.csv
  - write ./reports/latest/value_bets.csv (if odds given)
  - write ./reports/latest/summary.md
Exit codes:
  0 -> success with data
  2 -> ran, but no valid runners; writes 'empty' summary
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import pandas as pd
from parse_pdf import parse_forms
from odds_client import load_odds
from value import make_value_table

def ensure(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def build_probs(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["track","date","race","box","runner","prob_win"])
    rows = []
    for (t, d, r), grp in df.groupby(["track","date","race"], sort=True):
        n = max(len(grp), 1)
        p = 1.0 / n
        for _, g in grp.iterrows():
            rows.append({
                "track": t, "date": d, "race": r,
                "box": int(g["box"]), "runner": g["runner"],
                "prob_win": round(p, 6)
            })
    return (pd.DataFrame(rows)
            .sort_values(["track","date","race","box"])
            .reset_index(drop=True))

def write_summary(prob_df: pd.DataFrame, value_df: pd.DataFrame, dest: Path) -> None:
    if prob_df.empty:
        dest.write_text("## Summary — empty data\n", encoding="utf-8")
        return
    newest = sorted(prob_df["date"].unique())[-1]
    lines = [f"# Summary — {newest}\n", "Top pick per race (uniform baseline)\n"]
    for (t,d,r), grp in prob_df.groupby(["track","date","race"], sort=True):
        pick = grp.sort_values(["prob_win","box"], ascending=[False,True]).iloc[0]
        lines.append(f"- **{t}** R{r} → Box {int(pick['box'])} — {pick['runner']} (p={pick['prob_win']:.3f})")
    if value_df is not None and not value_df.empty:
        lines.append("\n## Value bets (Kelly, quarter fraction)\n")
        for _, row in value_df.iterrows():
            lines.append(
                f"- {row['date']} {row['track']} R{int(row['race'])} Box {int(row['box'])} "
                f"— {row['runner']} | p={row['prob_win']:.3f}, "
                f"odds={row['odds_decimal']:.2f}, edge={row['edge']:.3f}, "
                f"stake=${row['stake']:.2f}"
            )
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main() -> int:
    root = Path(__file__).resolve().parent.parent
    forms = root / "forms"
    rns_dir = root / "data" / "rns"
    latest = root / "reports" / "latest"
    ensure(rns_dir); ensure(latest)

    df = parse_forms(forms)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    parsed_csv = rns_dir / f"parsed_{ts}.csv"
    df.to_csv(parsed_csv, index=False)

    if df.empty:
        write_summary(pd.DataFrame(), pd.DataFrame(), latest / "summary.md")
        print("[run_daily] No valid runners parsed — wrote empty summary.")
        return 2

    probs = build_probs(df)
    probs.to_csv(latest / "probabilities.csv", index=False)

    # Optional: value bets if odds.csv exists
    odds = load_odds(forms / "odds.csv")
    value_df = make_value_table(probs, odds) if not odds.empty else pd.DataFrame()
    if not value_df.empty:
        value_df.to_csv(latest / "value_bets.csv", index=False)

    write_summary(probs, value_df, latest / "summary.md")

    print(f"[run_daily] parsed_rows={len(df)} probs_rows={len(probs)} value_rows={len(value_df)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

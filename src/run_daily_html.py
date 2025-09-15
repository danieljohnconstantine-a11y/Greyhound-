#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily HTML pipeline:
- discover meetings
- fetch each page and parse text for races/runners
- write parsed CSV under data/html/parsed_<UTC>.csv
- write reports/latest/{probabilities.csv, summary.md}
- exit 2 if nothing parsed
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
        return pd.DataFrame(columns=["track","date","race","box","runner","prob_win"])
    rows = []
    for (t,d,r), g in df.groupby(["track","date","race"], sort=True):
        n = max(len(g), 1)
        p = round(1.0/n, 6)
        for _, row in g.iterrows():
            rows.append({
                "track": t, "date": d, "race": int(r),
                "box": int(row["box"]), "runner": row["runner"], "prob_win": p
            })
    return (pd.DataFrame(rows)
            .sort_values(["track","race","box"])
            .reset_index(drop=True))

def write_summary(prob_df: pd.DataFrame, out_path: Path) -> None:
    if prob_df.empty:
        out_path.write_text("## Summary — empty data\n", encoding="utf-8")
        return
    newest = sorted(prob_df["date"].unique())[-1]
    lines = [f"# Summary — {newest}", "", "Top pick per race (uniform baseline)", ""]
    for (t,d,r), g in prob_df.groupby(["track","date","race"], sort=True):
        pick = g.sort_values(["prob_win","box"], ascending=[False,True]).iloc[0]
        lines.append(f"- **{t}** R{int(r)} → Box {int(pick['box'])} — {pick['runner']} (p={pick['prob_win']:.3f})")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main() -> int:
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data" / "html"
    reports = root / "reports" / "latest"
    ensure_dir(data_dir); ensure_dir(reports)

    df = fetch_and_parse_all(debug_root=root / "data" / "html" / "debug")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parsed_csv = data_dir / f"parsed_{ts}.csv"
    df.to_csv(parsed_csv, index=False)

    if df.empty:
        (reports / "summary.md").write_text("## Summary — empty data\n", encoding="utf-8")
        print("[html] No rows parsed — wrote empty summary.")
        return 2

    probs = build_probs(df)
    probs.to_csv(reports / "probabilities.csv", index=False)
    write_summary(probs, reports / "summary.md")

    print(f"[html] parsed_rows={len(df)}")
    print(f"[html] wrote: {parsed_csv}")
    print(f"[html] wrote: {reports / 'probabilities.csv'}")
    print(f"[html] wrote: {reports / 'summary.md'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

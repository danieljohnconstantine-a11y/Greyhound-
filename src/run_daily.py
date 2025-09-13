# src/run_daily.py
import os
import csv
from datetime import datetime, timezone
from typing import List, Dict

from parse_pdf import parse_forms_for_today, to_dicts
from features import build_features
from model import score_and_prob

REPORTS_DIR = "reports/latest"

def ensure_dirs():
    os.makedirs(REPORTS_DIR, exist_ok=True)

def write_csv(rows: List[Dict], path: str):
    if not rows:
        # still write headers for predictability
        cols = ["track","date","race","box","runner","trainer","field_size","box_prior","prob_win"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
        return
    cols = list({k for r in rows for k in r.keys()})
    core = ["track","date","race","box","runner","trainer","field_size","box_prior","prob_win"]
    # keep core first
    cols = core + [c for c in cols if c not in core]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_summary(rows: List[Dict], path: str):
    # best pick per race
    by_key: dict[tuple, List[Dict]] = {}
    for r in rows:
        key = (r["track"], r["date"], r["race"])
        by_key.setdefault(key, []).append(r)

    lines = []
    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    lines.append(f"# Summary — {today}\n")
    lines.append("Top pick per race (box-bias prior):\n")
    for key in sorted(by_key):
        track, date, race = key
        rs = sorted(by_key[key], key=lambda x: x.get("prob_win", 0), reverse=True)
        best = rs[0]
        runner = best.get("runner") or f"Box {best['box']}"
        p = best.get("prob_win", 0.0)
        trainer = best.get("trainer") or "-"
        lines.append(f"- **{track} R{race}** → **{runner}** (Box {best['box']}, Trainer: {trainer}) — p={p:.3f}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

def main():
    ensure_dirs()
    parsed = parse_forms_for_today("forms")
    parsed_dicts = to_dicts(parsed)
    feats = build_features(parsed_dicts)
    scored = score_and_prob(feats)
    write_csv(scored, os.path.join(REPORTS_DIR, "probabilities.csv"))
    write_summary(scored, os.path.join(REPORTS_DIR, "summary.md"))
    print(f"[done] rows={len(scored)} -> reports/latest/")

if __name__ == "__main__":
    main()

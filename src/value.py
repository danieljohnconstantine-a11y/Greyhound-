# src/value.py
# Usage: python src/value.py --in data/combined/features_*.csv --out reports/latest/bets_*.csv
import argparse, pandas as pd, numpy as np
from pathlib import Path

def kelly_fraction(p, b, f=0.25):
    """ Fractional Kelly. p=prob, b=odds-1. f in [0..1]. """
    edge = (b*p - (1-p))
    k = edge / b
    k = np.where((b>0)&(k>0), k, 0.0)
    return f * k

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True)
    ap.add_argument("--out", dest="out_csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)

    # Use market odds if present else fair_price as proxy (no bet if missing)
    has_odds = df["odds"].fillna(0).gt(0)
    b = np.where(has_odds, df["odds"].values - 1.0, df["fair_price"].values - 1.0)
    p = df["model_prob"].values

    df["stake_frac"] = kelly_fraction(p, b, f=0.25)
    df["stake_units"] = (df["stake_frac"] * 10).round(2)  # 10u bank per day (adjust)
    df["note"] = np.where(df["stake_units"]>0, "Bet", "No bet")

    # Only show selections with positive stake
    sel = df[df["stake_units"]>0].copy()
    sel = sel[[
        "meeting","race","time","box","runner","odds","fair_price","model_prob","edge","stake_units","note"
    ]].sort_values(["meeting","race","stake_units"], ascending=[True, True, False])

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    sel.to_csv(out, index=False)
    print(f"bets={len(sel)} -> {out}")

if __name__ == "__main__":
    main()

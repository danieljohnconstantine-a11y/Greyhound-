# src/features.py
# Usage: python src/features.py --in data/combined/forms_*.csv --out data/combined/features_*.csv
import argparse, pandas as pd, numpy as np
from pathlib import Path

def k_fair_from_odds(odds):
    # decimal odds -> implied prob
    with np.errstate(divide="ignore", invalid="ignore"):
        p = np.where(odds>0, 1.0/odds, np.nan)
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_csv", required=True)
    ap.add_argument("--out", dest="out_csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)
    # Normalise
    for col in ["meeting","track","grade","runner","trainer","source"]:
        if col in df: df[col] = df[col].astype(str).str.strip()

    # Coerce numerics
    for col in ["race","box","odds"]:
        if col in df: df[col] = pd.to_numeric(df[col], errors="coerce")

    # Implied prob from current odds (if present)
    if "odds" in df.columns:
        df["implied"] = k_fair_from_odds(df["odds"].values)
    else:
        df["implied"] = np.nan

    # Simple priors (placeholders—improve with history):
    # - Box bias: inside draws a tiny positive prior
    df["box_prior"] = np.where(df["box"].between(1,2), 0.02,
                        np.where(df["box"].between(3,4), 0.01,
                        np.where(df["box"].between(5,6), -0.005,
                        np.where(df["box"].between(7,8), -0.015, 0.0))))

    # - Grade prior (open/maiden heuristics)
    df["grade_prior"] = 0.0
    if "grade" in df:
        df.loc[df["grade"].str.contains("Open", case=False, na=False), "grade_prior"] = 0.01
        df.loc[df["grade"].str.contains("Maiden", case=False, na=False), "grade_prior"] = -0.02

    # Combine priors into prior_prob and blend with implied (if any)
    df["prior_prob"] = (0.5*df["box_prior"] + 0.5*df["grade_prior"]).clip(lower=-0.05, upper=0.05)
    df["model_prob"] = (df["implied"].fillna(0.14) + df["prior_prob"]).clip(0.02, 0.9)

    # Fair price, edge vs market odds (if odds exist)
    df["fair_price"] = 1.0 / df["model_prob"]
    df["edge"] = np.where(df["odds"].gt(0), df["odds"] / df["fair_price"] - 1.0, np.nan)

    # Rank by race
    df["race"] = df["race"].fillna(-1).astype(int)
    df = df.sort_values(["meeting","race","model_prob"], ascending=[True, True, False])
    df["rank"] = df.groupby(["meeting","race"])["model_prob"].rank(ascending=False, method="first")

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"features_rows={len(df)} -> {out}")

if __name__ == "__main__":
    main()

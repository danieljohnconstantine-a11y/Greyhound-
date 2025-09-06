from __future__ import annotations
import pandas as pd

def implied_prob(odds: float) -> float:
    return 0.0 if float(odds) <= 0 else 1.0 / float(odds)

def kelly_fraction(p: float, o: float, kelly: float = 0.25) -> float:
    b = float(o) - 1.0
    if b <= 0:
        return 0.0
    edge = (p * (b + 1)) - 1.0
    f = edge / b
    return max(0.0, f * kelly)

def find_value_bets(prob_df: pd.DataFrame, odds_df: pd.DataFrame, bank: float, kelly: float) -> pd.DataFrame:
    # odds_df must have: track,date,race,box,runner,odds
    merged = prob_df.merge(
        odds_df, on=["track", "date", "race", "box", "runner"], how="inner", suffixes=("", "_odds")
    )
    merged["implied"] = merged["odds"].astype(float).apply(implied_prob)
    merged["edge"] = merged["prob"] - merged["implied"]
    merged["stake_frac"] = merged.apply(lambda r: kelly_fraction(r["prob"], r["odds"], kelly), axis=1)
    merged["stake"] = (bank * merged["stake_frac"]).round(2)
    bets = merged[merged["edge"] > 0].sort_values(["race", "edge"], ascending=[True, False]).reset_index(drop=True)
    return bets

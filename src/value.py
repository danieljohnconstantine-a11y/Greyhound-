#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute simple value bets using uniform model probabilities.

We use fractional Kelly staking:
  kelly_fraction = max( (p * (b+1) - 1) / b, 0 ), where
  b = odds_decimal - 1

Environment variables (optional):
  BANKROLL        -> float, default 1000.0
  KELLY_FRACTION  -> between 0 and 1, default 0.25 (quarter Kelly)
  MIN_EDGE        -> minimum (p - breakeven_p) to include, default 0.01
"""

from __future__ import annotations
import os
import pandas as pd

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return default

def kelly_stake(prob: float, odds_decimal: float, bankroll: float, kelly_fraction: float) -> float:
    b = max(odds_decimal - 1.0, 0.0)
    if b <= 0:
        return 0.0
    edge_raw = (prob * (b + 1.0) - 1.0) / b  # classic Kelly
    k = max(edge_raw, 0.0) * kelly_fraction
    stake = max(k * bankroll, 0.0)
    return round(stake, 2)

def make_value_table(probs: pd.DataFrame, odds: pd.DataFrame) -> pd.DataFrame:
    if probs.empty or odds.empty:
        return pd.DataFrame(columns=["track","date","race","box","runner","prob_win","odds_decimal","breakeven_p","edge","stake"])

    bankroll = _env_float("BANKROLL", 1000.0)
    k_frac   = _env_float("KELLY_FRACTION", 0.25)
    min_edge = _env_float("MIN_EDGE", 0.01)

    merged = probs.merge(
        odds,
        on=["track","date","race","box"],
        how="inner",
        validate="many_to_one"
    )
    if merged.empty:
        return pd.DataFrame(columns=["track","date","race","box","runner","prob_win","odds_decimal","breakeven_p","edge","stake"])

    merged["breakeven_p"] = 1.0 / merged["odds_decimal"].clip(lower=1.01)
    merged["edge"] = merged["prob_win"] - merged["breakeven_p"]
    merged = merged[merged["edge"] >= min_edge].copy()
    if merged.empty:
        return merged.assign(stake=[])

    merged["stake"] = merged.apply(
        lambda r: kelly_stake(r["prob_win"], r["odds_decimal"], bankroll, k_frac), axis=1
    )
    merged = merged[merged["stake"] > 0].copy()
    merged = merged.sort_values(["date","track","race","edge"], ascending=[True, True, True, False])
    return merged.reset_index(drop=True)

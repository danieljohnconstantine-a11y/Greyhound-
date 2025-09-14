#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lightweight odds reader.

We avoid bookmaker scraping. Instead, if you place a CSV at ./forms/odds.csv
the pipeline will use it automatically.

Expected columns (header required), one row per runner:
track,date,race,box,odds_decimal

Example:
QSTR,2025-09-08,1,2,5.50
QSTR,2025-09-08,1,3,3.80
...

Notes:
- track: the same code as the PDF file prefix (e.g., QSTR, HEAL, SALE, etc.)
- date: YYYY-MM-DD (match the PDF date)
- race: integer
- box: 1..8
- odds_decimal: > 1.01
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd


COLUMNS = ["track","date","race","box","odds_decimal"]

def load_odds(csv_path: str | Path = "forms/odds.csv") -> pd.DataFrame:
    p = Path(csv_path)
    if not p.exists():
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_csv(p)
    # Normalise schema
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"odds.csv missing columns: {missing}")
    df = df[COLUMNS].copy()
    df["track"] = df["track"].astype(str).str.upper()
    df["date"] = df["date"].astype(str)
    df["race"] = df["race"].astype(int)
    df["box"] = df["box"].astype(int)
    df["odds_decimal"] = df["odds_decimal"].astype(float)
    df = df[(df["odds_decimal"] >= 1.01) & (df["box"].between(1,8))]
    return df.reset_index(drop=True)

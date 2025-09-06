from __future__ import annotations
from pathlib import Path
import pandas as pd

def load_odds_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    required = {"track", "date", "race", "box", "runner", "odds"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"odds.csv missing columns: {missing}")
    return df

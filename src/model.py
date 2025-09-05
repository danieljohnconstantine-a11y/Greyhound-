from __future__ import annotations
import pandas as pd
from pathlib import Path

# Placeholder for future calibration/history use
HISTORY_FILE = Path("data/history_prob.csv")

def predict_probabilities(features_df: pd.DataFrame) -> pd.DataFrame:
    """
    For now, probabilities are already computed in features.py (column 'prob').
    This function simply returns the dataframe unchanged so the rest of the
    pipeline has a stable interface. Later you can add calibration here.
    """
    return features_df.copy()

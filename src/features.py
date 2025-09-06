from __future__ import annotations
import re
import pandas as pd

# Basic form-score mapping if we spot recent form digits like 1-8 in the text
FORM_DIGIT_VAL = {"1": 1.00, "2": 0.65, "3": 0.45, "4": 0.25, "5": 0.15, "6": 0.10, "7": 0.05, "8": 0.02}

def _form_score(text: str | None) -> float:
    if not text:
        return 0.25
    s = re.sub(r"[^1-8]", "", text)  # keep only 1..8 digits if present
    if not s:
        return 0.25
    vals = [FORM_DIGIT_VAL.get(ch, 0.05) for ch in s[:5]]
    # weight recent more
    w = [1.0, 0.9, 0.8, 0.7, 0.6][: len(vals)]
    num = sum(v * wi for v, wi in zip(vals, w))
    den = sum(w)
    return num / den

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Simple box advantage: inside draws slightly favoured (box1 best, box8 worst)
    out["box_adv"] = (9 - out["box"]) / 8.0  # 1.0 .. 0.125

    # Create a combined text source for crude form extraction
    src = out["form_text"].fillna("") + " " + out["comments"].fillna("")
    out["form_score"] = src.apply(_form_score)

    # Simple composite score
    out["_score"] = 0.6 * out["form_score"] + 0.4 * out["box_adv"]

    # Convert to per-race probabilities via softmax
    def _softmax(series: pd.Series) -> pd.Series:
        import numpy as np
        z = series - series.max()
        e = np.exp(z)
        return e / e.sum()

    out["prob"] = out.groupby("race")["_score"].transform(_softmax)
    return out

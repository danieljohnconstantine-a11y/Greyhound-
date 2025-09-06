from __future__ import annotations
import re
from typing import Tuple
import pandas as pd
import numpy as np

"""
Feature engineering for greyhound runners.

Input columns expected (from parser):
- track (str)
- date (str)
- race (int)
- box (int 1..8)
- runner (str)
- comments (str, optional)
- form_text (str, optional)

Outputs (adds):
- box_adv, form_score, pace_early/rail/wide, _score, prob
"""

# ---- Configurable weights (tune later) --------------------------------------
WEIGHT_FORM = 0.60
WEIGHT_BOX = 0.35
WEIGHT_PACE = 0.05  # spread across pace flags

# Map recent finishing digits (1..8) to a score
FORM_DIGIT_VAL = {
    "1": 1.00, "2": 0.65, "3": 0.45, "4": 0.25,
    "5": 0.15, "6": 0.10, "7": 0.05, "8": 0.02
}

PACE_KW = {
    "early": ["quick early", "fast early", "good early", "box speed", "began well", "began fast", "early pace"],
    "rail":  ["rails", "hugged rail", "inside run", "drew the rail", "drawn inside"],
    "wide":  ["wide", "off the track", "posted wide", "drawn wide", "outer"]
}


def _form_score(text: str | None) -> float:
    """
    Build a crude 'form' score from any recent-run digits we can see.
    We keep only 1..8 digits (first 5), weight recent heavier.
    """
    if not text:
        return 0.25
    s = re.sub(r"[^1-8]", "", text)
    if not s:
        return 0.25
    digits = list(s[:5])  # last ~5 runs if present
    vals = [FORM_DIGIT_VAL.get(ch, 0.05) for ch in digits]
    # more recent runs weighted higher
    weights = [1.0, 0.9, 0.8, 0.7, 0.6][:len(vals)]
    num = float(sum(v * w for v, w in zip(vals, weights)))
    den = float(sum(weights)) if weights else 1.0
    return num / den


def _pace_flags(text: str | None) -> Tuple[int, int, int]:
    """
    Simple keyword flags for early/rail/wide tendencies.
    """
    t = (text or "").lower()
    early = int(any(k in t for k in PACE_KW["early"]))
    rail  = int(any(k in t for k in PACE_KW["rail"]))
    wide  = int(any(k in t for k in PACE_KW["wide"]))
    return early, rail, wide


def _safe_softmax(x: pd.Series) -> pd.Series:
    """

from __future__ import annotations
import re
from typing import Tuple
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------
# Feature engineering for greyhound runners
# -----------------------------------------------------------------------------

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
    Build a crude 'form' score from recent-run digits.
    """
    if not text:
        return 0.25
    s = re.sub(r"[^1-8]", "", text)
    if not s:
        return 0.25
    digits = list(s[:5])  # last ~5 runs if present
    vals = [FORM_DIGIT_VAL.get(ch, 0.05) for ch in digits]
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
    Numerically stable softmax over a 1D series.
    """
    if len(x) == 0:
        return x
    z = x - x.max()
    e = np.exp(z)
    denom = e.sum()
    if denom <= 0:
        return pd.Series([1.0 / len(x)] * len(x), index=x.index)
    return e / denom


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main feature builder. Returns a copy with:
      - box_adv, form_score, pace_* flags
      - _score (linear combo)
      - prob (softmax by track+race)
    """
    out = df.copy()

    # Ensure required columns exist
    for col in ["track", "race", "box", "runner"]:
        if col not in out.columns:
            out[col] = np.nan

    out["race"] = pd.to_numeric(out["race"], errors="coerce")
    out["box"]  = pd.to_numeric(out["box"], errors="coerce")
    out = out.dropna(subset=["track", "race", "box", "runner"])
    if out.empty:
        out["prob"] = []
        return out

    out["race"] = out["race"].astype(int, errors="ignore")
    out["box"]  = out["box"].astype(int, errors="ignore")

    # 1) Box advantage
    out["box_adv"] = (9 - out["box"]) / 8.0
    out.loc[~out["box"].between(1, 8), "box_adv"] = 0.25

    # 2) Form score
    src_text = out.get("form_text", "").astype(str).fillna("") + " " + out.get("comments", "").astype(str).fillna("")
    out["form_score"] = src_text.apply(_form_score)

    # 3) Pace flags
    flags = src_text.apply(_pace_flags)
    out["pace_early"] = flags.apply(lambda t: t[0])
    out["pace_rail"]  = flags.apply(lambda t: t[1])
    out["pace_wide"]  = flags.apply(lambda t: t[2])

    # 4) Composite score
    pace_weight_each = (WEIGHT_PACE / 2.0) if WEIGHT_PACE > 0 else 0.0
    out["_score"] = (
        WEIGHT_FORM * out["form_score"]
        + WEIGHT_BOX  * out["box_adv"]
        + pace_weight_each * out["pace_early"]
        + pace_weight_each * out["pace_rail"]
        - (WEIGHT_PACE * 0.5) * out["pace_wide"]
    )

    # 5) Convert to probabilities per (track, race)
    out["prob"] = out.groupby(["track", "race"])["_score"].transform(_safe_softmax)

    out = out.sort_values(["track", "race", "box"]).reset_index(drop=True)
    return out

# src/features.py
from typing import List, Dict
from collections import defaultdict

# Box bias prior (heuristic). Sums ≈1 for 8 runners.
# You can tune these numbers later per track.
BOX_PRIOR = [0.17, 0.16, 0.15, 0.13, 0.12, 0.10, 0.09, 0.08]  # index 0 -> box1

def build_features(rows: List[Dict]) -> List[Dict]:
    # Group by (track,date,race) to compute field size and normalize priors
    groups = defaultdict(list)
    for r in rows:
        key = (r["track"], r["date"], r["race"])
        groups[key].append(r)

    feats: list[Dict] = []
    for key, runners in groups.items():
        field_size = len(runners)
        # Normalize priors for actual field size
        priors = BOX_PRIOR[:field_size]
        s = sum(priors) if priors else 1.0
        norm_priors = [p / s for p in priors]

        for r in sorted(runners, key=lambda x: x["box"]):
            box_idx = max(1, min(8, int(r["box"]))) - 1
            prior = norm_priors[box_idx] if box_idx < len(norm_priors) else 1.0/field_size
            row = dict(r)
            row["field_size"] = field_size
            row["box_prior"] = prior
            feats.append(row)
    return feats

# src/model.py
from typing import List, Dict
from math import exp
from collections import defaultdict

def softmax(vals):
    mx = max(vals) if vals else 0.0
    exps = [exp(v - mx) for v in vals]
    s = sum(exps) or 1.0
    return [x/s for x in exps]

def score_and_prob(rows: List[Dict]) -> List[Dict]:
    # Group by race, softmax box_prior
    groups = defaultdict(list)
    for r in rows:
        key = (r["track"], r["date"], r["race"])
        groups[key].append(r)

    out: list[Dict] = []
    for key, runners in groups.items():
        scores = [r.get("box_prior", 0.0) for r in runners]
        probs = softmax(scores)
        for r, p in zip(runners, probs):
            row = dict(r)
            row["prob_win"] = round(p, 4)
            out.append(row)
    return out

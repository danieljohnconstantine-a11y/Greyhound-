import os, json, argparse
import pandas as pd

def load_probabilities(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def load_odds(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def compute_value_bets(probs: pd.DataFrame, odds_data: dict, min_edge: float = 0.05):
    """
    Compare model probabilities with market odds.
    A 'value bet' is when our prob > implied prob by margin `min_edge`.
    """
    rows = []
    for ev in odds_data.get("events", []):
        comp = (ev.get("bookmakers") or [{}])[0]
        for market in comp.get("markets", []):
            for outcome in market.get("outcomes", []):
                runner = outcome["name"]
                price = outcome.get("price")
                if not price:
                    continue
                implied = 1.0 / price

                # match against probs.csv
                match = probs[probs["runner"].str.contains(runner, case=False, na=False)]
                if match.empty:
                    continue
                model_p = match.iloc[0]["prob_win"]

                edge = model_p - implied
                if edge > min_edge:
                    rows.append({
                        "runner": runner,
                        "model_prob": round(model_p, 3),
                        "implied_prob": round(implied, 3),
                        "odds": price,
                        "edge": round(edge, 3)
                    })
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--probs", default="reports/latest/probabilities.csv")
    ap.add_argument("--odds", default="data/odds/today.json")
    ap.add_argument("--out", default="reports/latest/bets.csv")
    args = ap.parse_args()

    probs = load_probabilities(args.probs)
    odds = load_odds(args.odds)

    bets = compute_value_bets(probs, odds)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    bets.to_csv(args.out, index=False)

    print(f"Saved {len(bets)} recommended bets -> {args.out}")

if __name__ == "__main__":
    main()

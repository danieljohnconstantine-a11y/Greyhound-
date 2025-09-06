# src/make_bets.py
import argparse
import os
import json
import pandas as pd

def load_odds(odds_dir: str) -> pd.DataFrame:
    """Load all odds JSON files into a DataFrame."""
    rows = []
    for fname in os.listdir(odds_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(odds_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # expected Odds API format: events -> bookmakers -> markets -> outcomes
        for ev in data.get("events", []):
            sport = ev.get("sport_key")
            commence = ev.get("commence_time")
            for bm in ev.get("bookmakers", []):
                bookie = bm["title"]
                for market in bm.get("markets", []):
                    market_key = market.get("key")
                    for outcome in market.get("outcomes", []):
                        rows.append({
                            "sport": sport,
                            "commence_time": commence,
                            "bookmaker": bookie,
                            "market": market_key,
                            "name": outcome.get("name"),
                            "price": outcome.get("price"),
                        })
    return pd.DataFrame(rows)


def make_bets(prob_csv: str, odds_dir: str, out_csv: str):
    """Merge probabilities with odds and compute expected value."""
    probs = pd.read_csv(prob_csv)
    odds = load_odds(odds_dir)

    if probs.empty or odds.empty:
        print("No data available to make bets.")
        return

    # Simple join on runner name
    merged = probs.merge(
        odds, left_on="runner", right_on="name", how="inner"
    )

    # Expected Value (EV) = p_win * price
    merged["expected_value"] = merged["prob_win"] * merged["price"]

    # Only keep strong bets (EV > 1.05 for example)
    best = merged[merged["expected_value"] > 1.05].copy()
    best.sort_values("expected_value", ascending=False, inplace=True)

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    best.to_csv(out_csv, index=False)
    print(f"Saved bets to {out_csv}")


def parse_args():
    p = argparse.ArgumentParser(description="Generate bets from probs + odds")
    p.add_argument("--probs", required=True, help="Path to probabilities.csv")
    p.add_argument("--odds", required=True, help="Directory with odds JSONs")
    p.add_argument("--out", required=True, help="Output bets.csv")
    return p.parse_args()


def main():
    args = parse_args()
    make_bets(args.probs, args.odds, args.out)


if __name__ == "__main__":
    main()

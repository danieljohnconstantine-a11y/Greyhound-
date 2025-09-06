# src/odds_client.py
import argparse
import os
import requests
import json
import time


def fetch_odds(api_key: str, sport_key: str, regions: str, markets: str, date: str) -> dict:
    """
    Fetch odds from The Odds API.
    """
    url = (
        f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        f"?apiKey={api_key}&regions={regions}&markets={markets}"
        f"&oddsFormat=decimal&dateFormat=iso"
    )
    print(f"[info] Requesting: {url}")

    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def save_json(data: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"[info] Saved odds to {out_path}")


def parse_args():
    p = argparse.ArgumentParser(description="Fetch odds from The Odds API")
    p.add_argument("--api-key", required=True, help="API key for The Odds API")
    p.add_argument("--sport", required=True, help="Sport key (e.g. greyhound_racing_aus)")
    p.add_argument("--regions", default="au", help="Regions to include")
    p.add_argument("--markets", default="h2h", help="Markets to include")
    p.add_argument("--date", default="today", help="Date (today or YYYY-MM-DD)")
    p.add_argument("--out-dir", required=True, help="Directory to store results")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.sport or args.sport == "null":
        print("[error] No valid sport key provided. Please check data/sports.json.")
        return 2

    try:
        events = fetch_odds(args.api_key, args.sport, args.regions, args.markets, args.date)
    except requests.HTTPError as e:
        print(f"[error] HTTPError: {e}")
        return 2
    except Exception as e:
        print(f"[error] Fetch failed: {e}")
        return 2

    # Save timestamped dump
    stamp = time.strftime("%Y%m%d-%H%M%S")
    raw_path = os.path.join(args.out_dir, f"events-raw-{stamp}.json")
    save_json({"events": events}, raw_path)

    # Save stable (latest) dump
    stable_path = os.path.join(args.out_dir, "events.json")
    save_json({"events": events}, stable_path)

    print(f"[info] Fetched {len(events)} events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

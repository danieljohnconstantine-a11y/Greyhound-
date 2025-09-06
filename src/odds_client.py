import os, json, re, argparse, datetime
from typing import List, Dict, Any
import requests

BASE = "https://api.the-odds-api.com/v4"

def _get(url: str, params: Dict[str, Any]) -> Any:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def list_sports(api_key: str) -> List[Dict[str, Any]]:
    return _get(f"{BASE}/sports", {"apiKey": api_key})

def find_au_greyhound_sport_keys(sports: List[Dict[str, Any]]) -> List[str]:
    keys = []
    for s in sports:
        name = (s.get("title") or "") + " " + (s.get("group") or "")
        if re.search(r"greyhound|dogs", name, re.I):
            keys.append(s["key"])
    return keys

def fetch_odds_for_sport(api_key: str, sport_key: str, region: str = "au") -> List[Dict[str, Any]]:
    params = {"apiKey": api_key, "regions": region, "markets": "outrights"}
    try:
        return _get(f"{BASE}/sports/{sport_key}/odds", params)
    except requests.HTTPError:
        params["markets"] = "h2h"
        try:
            return _get(f"{BASE}/sports/{sport_key}/odds", params)
        except requests.HTTPError:
            return []

def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch AU greyhound odds (best effort).")
    ap.add_argument("--api-key", default=os.getenv("ODDS_API_KEY"), help="The Odds API key")
    ap.add_argument("--out-dir", default="data/odds", help="Folder to save odds JSON")
    ap.add_argument("--date", default="today", help='"today" or YYYY-MM-DD (used in filename)')
    args = ap.parse_args()

    if not args.api_key:
        print("No ODDS_API_KEY provided.")
        return 2

    ymd = datetime.datetime.now().strftime("%Y-%m-%d") if args.date == "today" else args.date
    os.makedirs(args.out_dir, exist_ok=True)

    sports = list_sports(args.api_key)
    gh_keys = find_au_greyhound_sport_keys(sports)

    all_events: Dict[str, Any] = {"fetched_at": datetime.datetime.utcnow().isoformat()+"Z",
                                  "sport_keys": gh_keys, "events": []}

    for key in gh_keys:
        events = fetch_odds_for_sport(args.api_key, key, region="au")
        all_events["events"].extend([{"sport_key": key, **e} for e in events])

    out_path = os.path.join(args.out_dir, f"{ymd}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(all_events, f, indent=2, ensure_ascii=False)
    print(f"Saved odds JSON -> {out_path}  (events: {len(all_events['events'])})")
    if not all_events["events"]:
        print("Note: No AU greyhound markets found by the API.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

# src/odds_client.py
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests

API_ROOT = "https://api.the-odds-api.com/v4"


def _today_aet() -> str:
    """YYYY-MM-DD in Australia Eastern Time (+10)."""
    tz = timezone(timedelta(hours=10))
    return datetime.now(tz).strftime("%Y-%m-%d")


def _norm_date(s: str) -> str:
    return _today_aet() if s.lower() == "today" else s


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch odds from The Odds API and save JSON.")
    p.add_argument("--api-key", required=True, help="The Odds API key")
    p.add_argument("--out-dir", default="data/odds", help="Output directory")
    p.add_argument("--date", default="today", help='YYYY-MM-DD or "today" (AET)')

    # Flexible filters (now supported)
    p.add_argument("--sport", default="greyhounds",
                   help='Sport key or hint (e.g. "greyhounds")')
    p.add_argument("--regions", default="au",
                   help="Comma-separated regions (e.g. au,uk,us)")
    p.add_argument("--markets", default="h2h",
                   help="Comma-separated markets (e.g. h2h,spreads,totals)")
    return p.parse_args()


def _get(url: str, params: Dict[str, Any]) -> Any:
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def resolve_sport_key(api_key: string, hint: str) -> Optional[str]:
    """
    Resolve a sport key from a human hint by listing /sports.
    Returns the first sport whose key or title contains the hint (case-insensitive).
    """
    try:
        sports = _get(f"{API_ROOT}/sports", {"apiKey": api_key})
    except Exception as e:
        print(f"[warn] Could not list sports: {e}", file=sys.stderr)
        return None

    h = hint.strip().lower().replace(" ", "_")
    for s in sports:
        key = (s.get("key") or "").lower()
        title = (s.get("title") or "").lower()
        if h in key or h in title:
            return s.get("key")
    return None


def fetch_odds(api_key: str, sport_key: str, regions: str, markets: str) -> List[Dict[str, Any]]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    url = f"{API_ROOT}/sports/{sport_key}/odds"
    return _get(url, params)  # list of events


def filter_events_by_date(events: List[Dict[str, Any]], ymd: str) -> List[Dict[str, Any]]:
    out = []
    for ev in events:
        ct = ev.get("commence_time", "")
        if isinstance(ct, str) and ct[:10] == ymd:
            out.append(ev)
    return out


def save_json(obj: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Saved {path}")


def main() -> int:
    args = parse_args()
    ymd = _norm_date(args.date)

    # Resolve sport key (fallback to cleaned hint if not found)
    sport_key = resolve_sport_key(args.api_key, args.sport) or args.sport.strip().lower().replace(" ", "_")
    print(f"Using sport key: {sport_key}")

    try:
        events = fetch_odds(args.api_key, sport_key, args.regions, args.markets)
    except requests.HTTPError as e:
        print(f"[error] HTTPError: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"[error] Fetch failed: {e}", file=sys.stderr)
        return 2

    # 1) raw dump with timestamp (for auditing)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    raw_path = os.path.join(args.out_dir, f"{sport_key}_{stamp}.json")
    save_json({"events": events}, raw_path)

    # 2) stable, date-filtered dump used by downstream steps
    filtered = filter_events_by_date(events, ymd)
    stable_path = os.path.join(args.out_dir, f"{sport_key}_{ymd}.json")
    save_json({"events": filtered}, stable_path)

    print(f"Fetched events: {len(events)} | For {ymd}: {len(filtered)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

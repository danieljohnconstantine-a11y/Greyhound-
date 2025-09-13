#!/usr/bin/env python3
import argparse, json, os, glob, csv
from datetime import datetime, timezone

def utcstamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def newest(pattern: str):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None

def load_meetings(dirpath: str):
    """Load the newest meetings_*.json from dirpath; return list of dicts."""
    f = newest(os.path.join(dirpath, "meetings_*.json"))
    if not f:
        return []
    try:
        with open(f, "r", encoding="utf-8") as j:
            data = json.load(j)
            return data.get("meetings", []) or []
    except Exception:
        return []

def main(rns_dir: str, dogs_dir: str, out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    ts = utcstamp()

    rns = load_meetings(rns_dir)
    dogs = load_meetings(dogs_dir)

    # Combine & de-dupe by URL
    seen, combined = set(), []
    for src in (rns, dogs):
        for m in src:
            url = (m.get("url") or "").strip()
            key = url or m.get("name", "")
            if key and key not in seen:
                seen.add(key)
                combined.append(m)

    # Save combined JSON
    out_json_path = os.path.join(out_dir, f"full_day_{ts}.json")
    with open(out_json_path, "w", encoding="utf-8") as f:
        json.dump({
            "fetched_at_utc": ts,
            "meetings": combined,
            "counts": {"rns": len(rns), "thedogs": len(dogs), "combined": len(combined)}
        }, f, ensure_ascii=False, indent=2)

    # Save CSV (header-only if no rows yet)
    out_csv_path = os.path.join(out_dir, f"full_day_{ts}.csv")
    headers = ["meeting", "race", "time", "box", "runner", "odds", "meeting_url"]
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        # No race rows yet — this is the scaffold file downstream code can append to.

    print(f"combined_meetings={len(combined)}  rns={len(rns)}  thedogs={len(dogs)}")
    return 0

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rns", required=True)
    p.add_argument("--dogs", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()
    raise SystemExit(main(args.rns, args.dogs, args.out))

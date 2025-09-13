#!/usr/bin/env python3
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import json

def newest(pattern: str):
    files = sorted(Path().glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def load_latest_json(dirpath: str):
    j = newest(f"{dirpath}/meetings_*.json")
    if not j:
        return []
    data = json.loads(j.read_text(encoding="utf-8"))
    races = data.get("races", [])
    rows = []
    for r in races:
        for runner in r.get("runners", []):
            rows.append({
                "source": data.get("source", "unknown"),
                "meeting": r.get("meeting"),
                "meeting_url": r.get("meeting_url"),
                "race_no": r.get("race_no"),
                "time_local": r.get("time_local"),
                "runner_no": runner.get("number"),
                "runner_name": runner.get("name"),
                "box": runner.get("box"),
                "odds": runner.get("odds"),
                "trainer": runner.get("trainer"),
            })
    return rows

def main(rns_dir: str, dogs_dir: str, out_dir: str):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    rows = []
    rows += load_latest_json(rns_dir)
    rows += load_latest_json(dogs_dir)
    if not rows:
        # fallback: combine latest CSVs if exist
        dfs = []
        rns_csv = newest(f"{rns_dir}/full_day_*.csv")
        dogs_csv = newest(f"{dogs_dir}/full_day_*.csv")
        if rns_csv: dfs.append(pd.read_csv(rns_csv))
        if dogs_csv: dfs.append(pd.read_csv(dogs_csv))
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = pd.DataFrame(columns=["source","meeting","meeting_url","race_no","time_local","runner_no","runner_name","box","odds","trainer"])
    else:
        df = pd.DataFrame(rows)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df.to_csv(out / f"combined_{ts}.csv", index=False)
    print(f"[merge] rows={len(df)} -> {out}/combined_{ts}.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rns-dir", required=True)
    ap.add_argument("--dogs-dir", required=True)
    ap.add_argument("--out-dir", default="data/combined")
    args = ap.parse_args()
    main(args.rns_dir, args.dogs_dir, args.out_dir)

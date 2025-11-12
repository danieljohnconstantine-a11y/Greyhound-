#!/usr/bin/env python3
import os
import re
import pandas as pd
from pdfminer.high_level import extract_text

FNAME_RE = re.compile(r"^([A-Z]{4})_(\d{4}-\d{2}-\d{2})\.pdf$")

# simple patterns for dog lines and race headers
RACE_HEADER = re.compile(r"\b(Race\s*No\.?\s*|Race\s*)(\d+)\b", re.IGNORECASE)
# Match lines like "1. 54223 Zombie Boss" or "1. FAST PUP"
DOG_LINE = re.compile(r"^\s*([1-8])\.\s*(?:\d+\s+)?([A-Za-z0-9\'\- ]{2,})\s*$")

def parse_pdf(path: str) -> list[dict]:
    fn = os.path.basename(path)
    m = FNAME_RE.match(fn)
    if not m:
        return []
    track, date_str = m.group(1), m.group(2)

    try:
        text = extract_text(path)
    except Exception:
        return []

    rows: list[dict] = []
    current_race = None
    lines = text.splitlines()

    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line:
            continue

        # Race header can be "Race No" followed by number on next line, or "Race 1" style
        if line.lower() == "race no" and i + 1 < len(lines):
            # Check next non-empty line for race number
            for j in range(i + 1, min(i + 4, len(lines))):
                next_line = lines[j].strip()
                if next_line and next_line.isdigit():
                    try:
                        current_race = int(next_line)
                    except Exception:
                        pass
                    break
            continue
        
        # Also check traditional "Race 1" or "Race No. 1" style
        rh = RACE_HEADER.search(line)
        if rh:
            try:
                current_race = int(rh.group(2))
            except Exception:
                pass
            continue

        # dog line like "1. 54223 Zombie Boss" or "1. FAST PUP"
        dm = DOG_LINE.match(line)
        if dm and current_race is not None:
            box = int(dm.group(1))
            name = dm.group(2).strip().replace("  ", " ")
            rows.append({
                "track": track,
                "date": date_str,
                "race": current_race,
                "box": box,
                "runner": name
            })

    return rows

def parse_folder(forms_dir: str) -> pd.DataFrame:
    all_rows: list[dict] = []
    for fn in sorted(os.listdir(forms_dir)):
        if not fn.endswith(".pdf"):
            continue
        if not FNAME_RE.match(fn):
            # skip non-race PDFs
            continue
        path = os.path.join(forms_dir, fn)
        rows = parse_pdf(path)
        all_rows.extend(rows)

    return pd.DataFrame(all_rows, columns=["track","date","race","box","runner"])

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--forms", default="forms")
    ap.add_argument("--out", default="data/rns/parsed.csv")
    args = ap.parse_args()

    df = parse_folder(args.forms)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"[parse] rows={len(df)} wrote={args.out}")

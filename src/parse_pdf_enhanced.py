#!/usr/bin/env python3
"""
Enhanced PDF parser for greyhound racing forms.
Extracts detailed information for each dog including race history,
statistics, and performance metrics.
"""
import os
import re
import json
import pandas as pd
from typing import Dict, List, Optional, Any
from pdfminer.high_level import extract_text

FNAME_RE = re.compile(r"^([A-Z]{4})_(\d{4}-\d{2}-\d{2})\.pdf$")

# Pattern for race headers
RACE_HEADER = re.compile(r"\b(Race\s*No\.?\s*|Race\s*)(\d+)\b", re.IGNORECASE)

# Pattern for detailed dog section start (e.g., "1. ZOMBIE BOSS")
DOG_DETAIL_START = re.compile(r"^(\d+)\.\s*$")

# Pattern for dog name and basic info (e.g., "ZOMBIE BOSS")
DOG_NAME_LINE = re.compile(r"^([A-Z0-9\'\- ]+)$")

# Pattern for weight, box, color info (e.g., "0kg (1) blu 3 B")
DOG_INFO_LINE = re.compile(r"^([\d.]+)kg\s+\((\d+)\)\s+(.+?)\s+(\d+)\s+([DBWH])$")

# Pattern for pedigree (e.g., "FERNANDO BALE (AUS) - FARMOR BANGS (AUS)")
PEDIGREE_LINE = re.compile(r"^(.+?)\s+\([A-Z]+\)\s*-\s*(.+?)\s+\([A-Z]+\)$")

# Pattern for distances (e.g., "Raced Distance: 312-383      Winning Distance: 312m (4)")
DISTANCE_LINE = re.compile(r"Raced Distance:\s*([\d-]+)\s+Winning Distance:\s*(.+)$")

# Pattern for owner (e.g., "Owner: Adam Poulter")
OWNER_LINE = re.compile(r"^Owner:\s*(.+)$")

# Pattern for trainer name (standalone, e.g., "ADAM POULTER")
TRAINER_LINE = re.compile(r"^([A-Z\s]+)$")

# Pattern for Horse career stats (e.g., "Horse: 4-15-46 9%-42%")
HORSE_STATS = re.compile(r"Horse:\s*([\d-]+)\s+([\d%-]+)")

# Pattern for statistics values (e.g., "4-20-50" or "$143" or "0.1")
STAT_VALUE = re.compile(r"^([\d-]+|[\$\d,]+|\d+\.\d+)$")

# Pattern for percentage values (e.g., "8%-48%")
PERCENTAGE = re.compile(r"^([\d%-]+)$")

# Pattern for race history entries (e.g., "3rd of 5 20/08/2025 DARWIN...")
RACE_HISTORY = re.compile(
    r"^(\d+(?:st|nd|rd|th))\s+of\s+(\d+)\s+(\d{2}/\d{2}/\d{4})\s+([A-Z\s]+)\s+Margin\s+([\d.]+)"
)


def safe_extract(pattern: re.Pattern, text: str, group: int = 1, default: str = "") -> str:
    """Safely extract regex group or return default."""
    match = pattern.search(text)
    return match.group(group) if match else default


def parse_stat_line(lines: List[str], idx: int) -> tuple:
    """Parse a two-line statistic (label on first line, value on second)."""
    if idx + 1 < len(lines):
        label = lines[idx].strip()
        value = lines[idx + 1].strip()
        return label, value, idx + 2
    return None, None, idx + 1


def parse_dog_details(lines: List[str], start_idx: int) -> Optional[Dict[str, Any]]:
    """
    Parse detailed information for a single dog starting from start_idx.
    Returns dict with all extracted information and the next line index.
    """
    dog_info = {
        "box": None,
        "name": None,
        "weight": None,
        "color": None,
        "age": None,
        "sex": None,
        "trainer": None,
        "sire": None,
        "dam": None,
        "raced_distance": None,
        "winning_distance": None,
        "owner": None,
        "horse_record": None,
        "horse_win_pct": None,
        "horse_place_pct": None,
        "jockey_50s": None,
        "jockey_350s": None,
        "trainer_50s": None,
        "trainer_50s_pct": None,
        "trainer_350s": None,
        "trainer_350s_pct": None,
        "car_pm_per_start": None,
        "twelve_month_pm_per_start": None,
        "api": None,
        "rtc_per_km": None,
        "rdist_tc": None,
        "dls": None,
        "dlw": None,
        "dod": None,
        "grade_g1": None,
        "grade_g2": None,
        "grade_g3": None,
        "grade_lr": None,
        "grade_fu": None,
        "grade_2u": None,
        "grade_3u": None,
        "condition_firm": None,
        "condition_good": None,
        "condition_soft": None,
        "condition_heavy": None,
        "condition_aw": None,
        "condition_turf": None,
        "car_record": None,
        "twelve_month_record": None,
        "course_record": None,
        "distance_record": None,
        "clock_w_record": None,
        "aclock_w_record": None,
        "race_history": []
    }
    
    idx = start_idx
    
    # Skip the box number line (e.g., "1.")
    if idx < len(lines) and DOG_DETAIL_START.match(lines[idx].strip()):
        box_match = DOG_DETAIL_START.match(lines[idx].strip())
        if box_match:
            dog_info["box"] = int(box_match.group(1))
        idx += 1
        
        # Skip empty line
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
    
    # Parse dog name
    if idx < len(lines):
        name_line = lines[idx].strip()
        if DOG_NAME_LINE.match(name_line):
            dog_info["name"] = name_line
            idx += 1
    
    # Parse weight, box, color, age, sex (e.g., "0kg (1) blu 3 B")
    if idx < len(lines):
        info_match = DOG_INFO_LINE.match(lines[idx].strip())
        if info_match:
            dog_info["weight"] = info_match.group(1)
            if not dog_info["box"]:  # Use from detail line if not already set
                dog_info["box"] = int(info_match.group(2))
            dog_info["color"] = info_match.group(3)
            dog_info["age"] = info_match.group(4)
            dog_info["sex"] = info_match.group(5)
            idx += 1
    
    # Parse pedigree
    if idx < len(lines):
        pedigree_match = PEDIGREE_LINE.match(lines[idx].strip())
        if pedigree_match:
            dog_info["sire"] = pedigree_match.group(1).strip()
            dog_info["dam"] = pedigree_match.group(2).strip()
            idx += 1
    
    # Parse distances (may span multiple lines)
    if idx < len(lines):
        dist_match = DISTANCE_LINE.match(lines[idx].strip())
        if dist_match:
            dog_info["raced_distance"] = dist_match.group(1).strip()
            winning_dist = dist_match.group(2).strip()
            idx += 1
            
            # Winning distance may continue on next line(s)
            while idx < len(lines):
                next_line = lines[idx].strip()
                # Check if line continues distance info (starts with lowercase or digit, contains "m")
                if next_line and not next_line.startswith('Owner:') and not next_line.isupper():
                    if 'm' in next_line or next_line[0].isdigit():
                        winning_dist += " " + next_line
                        idx += 1
                    else:
                        break
                else:
                    break
            
            dog_info["winning_distance"] = winning_dist
    
    # Parse owner
    if idx < len(lines):
        owner_match = OWNER_LINE.match(lines[idx].strip())
        if owner_match:
            owner_text = owner_match.group(1).strip()
            # May continue on next line(s)
            idx += 1
            while idx < len(lines) and lines[idx].strip() and not TRAINER_LINE.match(lines[idx].strip()):
                owner_text += " " + lines[idx].strip()
                idx += 1
            dog_info["owner"] = owner_text
    
    # Skip empty lines
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    
    # Parse trainer name
    if idx < len(lines):
        trainer_line = lines[idx].strip()
        if trainer_line and trainer_line.isupper() and len(trainer_line.split()) <= 4:
            dog_info["trainer"] = trainer_line
            idx += 1
    
    # Skip empty lines
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    
    # Parse Horse career stats
    if idx < len(lines):
        horse_match = HORSE_STATS.match(lines[idx].strip())
        if horse_match:
            dog_info["horse_record"] = horse_match.group(1)
            pcts = horse_match.group(2)
            if "%" in pcts:
                parts = pcts.split("-")
                if len(parts) >= 2:
                    dog_info["horse_win_pct"] = parts[0].replace("%", "")
                    dog_info["horse_place_pct"] = parts[1].replace("%", "")
            idx += 1
    
    # Look for "J/T:" line
    while idx < len(lines) and "J/T:" not in lines[idx]:
        idx += 1
    
    if idx < len(lines) and "J/T:" in lines[idx]:
        idx += 1
        
        # Skip empty lines
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
        
        # Parse j50s, j350s, t50s, t350s statistics
        # These come in pairs: label line, then value line(s)
        stats_to_parse = ["j50s", "j350s", "t50s", "t350s"]
        stats_dict = {}
        
        for _ in range(20):  # Parse multiple stat pairs
            if idx >= len(lines):
                break
            
            line = lines[idx].strip()
            
            # Check if it's a known stat label
            if line in stats_to_parse:
                label = line
                idx += 1
                
                # Get value(s) on next line(s)
                values = []
                while idx < len(lines):
                    val_line = lines[idx].strip()
                    if not val_line:
                        idx += 1
                        continue
                    if val_line in stats_to_parse or val_line.startswith("Car") or val_line == "G1":
                        break
                    values.append(val_line)
                    idx += 1
                
                stats_dict[label] = " ".join(values)
            else:
                break
        
        dog_info["jockey_50s"] = stats_dict.get("j50s", "-")
        dog_info["jockey_350s"] = stats_dict.get("j350s", "-")
        dog_info["trainer_50s"] = stats_dict.get("t50s", "-")
        dog_info["trainer_350s"] = stats_dict.get("t350s", "-")
        
        # Parse trainer percentages from t50s and t350s
        if dog_info["trainer_50s"] and dog_info["trainer_50s"] != "-":
            parts = dog_info["trainer_50s"].split()
            if len(parts) >= 2 and "%" in parts[1]:
                dog_info["trainer_50s_pct"] = parts[1]
        
        if dog_info["trainer_350s"] and dog_info["trainer_350s"] != "-":
            parts = dog_info["trainer_350s"].split()
            if len(parts) >= 2 and "%" in parts[1]:
                dog_info["trainer_350s_pct"] = parts[1]
    
    # Parse remaining statistics (CarPM/s, API, RTC/km, etc.)
    # These also come in label-value pairs
    stat_mapping = {
        "CarPM/s": "car_pm_per_start",
        "12mPM/s": "twelve_month_pm_per_start",
        "API": "api",
        "DLS": "dls",
        "DLW": "dlw",
        "DOD": "dod",
        "G1": "grade_g1",
        "G2": "grade_g2",
        "G3": "grade_g3",
        "LR": "grade_lr",
        "FU": "grade_fu",
        "2U": "grade_2u",
        "3U": "grade_3u",
        "Firm": "condition_firm",
        "Good": "condition_good",
        "Soft": "condition_soft",
        "Heavy": "condition_heavy",
        "AW": "condition_aw",
        "Turf": "condition_turf",
        "Car": "car_record",
        "12m": "twelve_month_record",
        "Crs": "course_record",
        "Dist": "distance_record",
        "ClockW": "clock_w_record",
        "AClockW": "aclock_w_record"
    }
    
    # Continue parsing until we hit race history
    while idx < len(lines):
        line = lines[idx].strip()
        
        # Check if we've hit race history
        if RACE_HISTORY.match(line):
            break
        
        # Special handling for "RTC/km RDistTC" which appears on one line
        if line.startswith("RTC/km"):
            idx += 1
            # Get the combined value on next line (e.g., "47/15.133")
            if idx < len(lines):
                rtc_value = lines[idx].strip()
                if "/" in rtc_value:
                    parts = rtc_value.split("/")
                    if len(parts) >= 2:
                        dog_info["rtc_per_km"] = parts[0]
                        dog_info["rdist_tc"] = parts[1]
                idx += 1
            # Next line might have additional value
            if idx < len(lines) and lines[idx].strip() and lines[idx].strip().isdigit():
                dog_info["rdist_tc"] = lines[idx].strip()
                idx += 1
            continue
        
        # Check if line is a stat label
        if line in stat_mapping:
            label = line
            field_name = stat_mapping[label]
            idx += 1
            
            # Get value(s) on next line(s)
            values = []
            max_values = 2  # Most stats have at most 2 lines
            
            while idx < len(lines) and len(values) < max_values:
                val_line = lines[idx].strip()
                if not val_line:
                    idx += 1
                    continue
                # Stop if we hit another label or race history
                if val_line in stat_mapping or RACE_HISTORY.match(val_line) or val_line.startswith("RTC/km"):
                    break
                values.append(val_line)
                idx += 1
            
            dog_info[field_name] = " ".join(values) if values else "-"
        else:
            idx += 1
    
    # Parse race history
    while idx < len(lines):
        line = lines[idx].strip()
        
        # Check for new dog section
        if DOG_DETAIL_START.match(line):
            break
        
        # Check for race history entry
        if RACE_HISTORY.match(line):
            # Race history entries can span multiple lines
            race_entry = line
            idx += 1
            
            # Continue reading until we hit empty line or new entry
            while idx < len(lines):
                next_line = lines[idx].strip()
                if not next_line or RACE_HISTORY.match(next_line) or DOG_DETAIL_START.match(next_line):
                    break
                race_entry += " " + next_line
                idx += 1
            
            dog_info["race_history"].append(race_entry)
        else:
            idx += 1
    
    return dog_info, idx


def parse_pdf_enhanced(path: str) -> List[Dict[str, Any]]:
    """
    Enhanced parser that extracts detailed dog information.
    Returns list of dictionaries, one per dog with all available fields.
    """
    fn = os.path.basename(path)
    m = FNAME_RE.match(fn)
    if not m:
        return []
    
    track, date_str = m.group(1), m.group(2)
    
    try:
        text = extract_text(path)
    except Exception as e:
        print(f"Error extracting text from {path}: {e}")
        return []
    
    lines = text.split('\n')
    rows: List[Dict[str, Any]] = []
    current_race = None
    
    idx = 0
    while idx < len(lines):
        line = lines[idx].strip()
        
        # Check for race header - "Race No" may be on one line, number on next
        rh = RACE_HEADER.search(line)
        if rh:
            try:
                current_race = int(rh.group(2))
            except Exception:
                pass
        elif line == "Race No" and idx + 2 < len(lines):
            # Look ahead for race number
            next_line = lines[idx + 1].strip()
            next_next = lines[idx + 2].strip()
            if not next_line and next_next.isdigit():
                current_race = int(next_next)
            elif next_line.isdigit():
                current_race = int(next_line)
        
        # Check for dog detail section
        if DOG_DETAIL_START.match(line) and current_race is not None:
            dog_info, next_idx = parse_dog_details(lines, idx)
            if dog_info and dog_info.get("name"):
                # Add metadata
                dog_info["track"] = track
                dog_info["date"] = date_str
                dog_info["race"] = current_race
                rows.append(dog_info)
                idx = next_idx
                continue
        
        idx += 1
    
    return rows


def parse_folder_enhanced(forms_dir: str) -> pd.DataFrame:
    """Parse all PDFs in a folder with enhanced extraction."""
    all_rows: List[Dict[str, Any]] = []
    
    for fn in sorted(os.listdir(forms_dir)):
        if not fn.endswith(".pdf"):
            continue
        if not FNAME_RE.match(fn):
            continue
        
        path = os.path.join(forms_dir, fn)
        print(f"Parsing {fn}...")
        rows = parse_pdf_enhanced(path)
        all_rows.extend(rows)
        print(f"  Extracted {len(rows)} dogs")
    
    return pd.DataFrame(all_rows)


if __name__ == "__main__":
    import argparse
    
    ap = argparse.ArgumentParser(
        description="Enhanced PDF parser for greyhound racing forms"
    )
    ap.add_argument("--forms", default="forms", help="Directory containing PDF forms")
    ap.add_argument("--out", default="data/rns/parsed_enhanced.csv", 
                    help="Output CSV file")
    ap.add_argument("--json", default="data/rns/parsed_enhanced.json",
                    help="Output JSON file (includes race history)")
    ap.add_argument("--sample", action="store_true",
                    help="Process only first PDF for testing")
    args = ap.parse_args()
    
    # Get list of PDFs
    pdfs = sorted([f for f in os.listdir(args.forms) 
                   if f.endswith(".pdf") and FNAME_RE.match(f)])
    
    if args.sample and pdfs:
        pdfs = pdfs[:1]
        print(f"Sample mode: processing only {pdfs[0]}")
    
    all_rows = []
    for fn in pdfs:
        path = os.path.join(args.forms, fn)
        print(f"Parsing {fn}...")
        rows = parse_pdf_enhanced(path)
        all_rows.extend(rows)
        print(f"  Extracted {len(rows)} dogs")
    
    if all_rows:
        # Save as CSV
        df = pd.DataFrame(all_rows)
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f"\n[parse_enhanced] Saved {len(df)} rows to {args.out}")
        
        # Save as JSON (preserves race history as list)
        os.makedirs(os.path.dirname(args.json), exist_ok=True)
        with open(args.json, 'w') as f:
            json.dump(all_rows, f, indent=2)
        print(f"[parse_enhanced] Saved detailed data to {args.json}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"  Total dogs: {len(df)}")
        print(f"  Tracks: {df['track'].nunique()}")
        print(f"  Races: {df.groupby(['track', 'date'])['race'].nunique().sum()}")
        
        # Show sample of first dog
        if len(all_rows) > 0:
            print(f"\nSample (first dog):")
            first_dog = all_rows[0]
            for key, value in first_dog.items():
                if key == "race_history":
                    print(f"  {key}: {len(value)} entries")
                    if value:
                        print(f"    First: {value[0][:100]}...")
                else:
                    print(f"  {key}: {value}")
    else:
        print("No data extracted")

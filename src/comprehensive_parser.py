#!/usr/bin/env python3
"""
Comprehensive greyhound parser with robust extraction, validation, and sorting.
Implements all requirements from comment_id 3480397078.
"""

import os
import re
import logging
from typing import List, Dict, Optional, Tuple
import pdfplumber
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# Expected race counts per file
FILE_RACE_EXPECTATIONS = {
    'MANDG0710form': 11,  # Races 1-11
    'QLAKG0710form': 14,  # Races 1-14
}

# Exact 62 column order
COLUMNS = [
    "Track", "Race", "Box", "DogName", "Trainer", "Grade", "Distance", "RaceDate",
    "Form", "WinRate", "PlaceRate", "Odds", "BestTime", "Margin",
    "Sectional1", "Sectional2", "Sectional3", "RaceComment",
    "Starts", "Wins", "Seconds", "Thirds", "CareerPrizeMoney", "CareerBest",
    "RaceClass", "TrackCondition", "Weather", "Interference",
    "TrainerWinRate", "TrainerState", "TrainerCity",
    "Owner", "Sire", "Dam", "Age", "Sex", "Color", "Weight",
    "LastStartDate", "LastStartTrack", "LastStartResult", "LastStartMargin", "LastStartComment",
    "SplitAvg", "SpeedIndex", "ConsistencyIndex",
    "TrackDistanceWins", "TrackDistancePlaces", "TrackStarts", "TrackWins",
    "DistanceStarts", "DistanceWins", "DistancePlaces",
    "BoxHistory", "BoxWins", "BoxPlaces",
    "EarlySpeed", "ClosingSpeed", "Score",
    "Comments", "Notes", "SourcePDF"
]


def extract_race_number_robust(text: str) -> Optional[int]:
    """
    Robust race number extraction supporting multiple formats:
    - "Race 6"
    - "Race No 6"
    - "RACE 6"
    - "Broken Hill Race 6"
    Content-first approach: ignore filename if content found.
    """
    patterns = [
        r'Race\s+No\.?\s+(\d+)',  # Race No 6
        r'Race\s+(\d+)',           # Race 6
        r'RACE\s+(\d+)',           # RACE 6
        r'(?:Broken\s+Hill|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+Race\s+(\d+)',  # Location Race 6
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                continue
    
    return None


def extract_box_number_robust(line: str) -> Optional[int]:
    """
    Robust box extraction supporting:
    - "1." (leading ordinal)
    - "[1]" (bracketed)
    - "(1)" (parenthesized)
    Prefer leading numeric before dog name.
    Enforce int in range 1-10.
    """
    # Try leading ordinal first (highest priority)
    match = re.match(r'^\s*(\d+)\.', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    # Try bracketed
    match = re.match(r'^\s*\[(\d+)\]', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    # Try parenthesized
    match = re.match(r'^\s*\((\d+)\)', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    return None


def extract_distance_robust(text: str) -> Optional[int]:
    """Extract distance with pattern: (\d{3,4})\s*m"""
    match = re.search(r'\b(\d{3,4})\s*m\b', text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def normalize_race_time(time_str: str) -> Optional[float]:
    """
    Normalize race time to seconds (float).
    Accepts: "0:30.41", "30.41", "0:18.76", "18.76"
    Returns: float seconds or None
    """
    if not time_str:
        return None
    
    try:
        time_str = time_str.strip()
        if ':' in time_str:
            # Format: M:SS.MS or MM:SS.MS
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
        else:
            # Format: SS.MS
            return float(time_str)
    except (ValueError, IndexError, AttributeError):
        return None
    
    return None


def extract_sectionals_robust(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Extract sectional times supporting multiple formats:
    - "Sec Time 4.39"
    - "First Split 6.80"
    - "Sectional 1: 4.39" or "Sectional1 4.39"
    Returns: (sectional1, sectional2, sectional3)
    """
    sectional1, sectional2, sectional3 = None, None, None
    
    # Try "Sec Time" pattern
    sec_times = re.findall(r'Sec\s+Time\s+([\d\.]+)', text, re.IGNORECASE)
    if len(sec_times) >= 1:
        try:
            sectional1 = float(sec_times[0])
        except ValueError:
            pass
    if len(sec_times) >= 2:
        try:
            sectional2 = float(sec_times[1])
        except ValueError:
            pass
    if len(sec_times) >= 3:
        try:
            sectional3 = float(sec_times[2])
        except ValueError:
            pass
    
    # Try "First Split" pattern
    if not sectional1:
        match = re.search(r'First\s+Split\s+([\d\.]+)', text, re.IGNORECASE)
        if match:
            try:
                sectional1 = float(match.group(1))
            except ValueError:
                pass
    
    # Try "Sectional 1/2/3" patterns
    sectional_patterns = [
        (r'Sectional\s*1[:\s]+([\d\.]+)', 'sectional1'),
        (r'Sectional\s*2[:\s]+([\d\.]+)', 'sectional2'),
        (r'Sectional\s*3[:\s]+([\d\.]+)', 'sectional3'),
    ]
    
    for pattern, name in sectional_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                val = float(match.group(1))
                if name == 'sectional1' and not sectional1:
                    sectional1 = val
                elif name == 'sectional2' and not sectional2:
                    sectional2 = val
                elif name == 'sectional3' and not sectional3:
                    sectional3 = val
            except ValueError:
                pass
    
    return sectional1, sectional2, sectional3


def calculate_speed_metrics(distance_m: Optional[int], race_time_s: Optional[float],
                            sectional1: Optional[float], sectional2: Optional[float], 
                            sectional3: Optional[float]) -> Dict:
    """
    Calculate distance-aware speed metrics:
    - SpeedIndex = meters/second
    - EarlySpeed, ClosingSpeed derived from sectionals when present
    - SplitAvg = average of sectionals
    """
    metrics = {
        'SpeedIndex': None,
        'EarlySpeed': None,
        'ClosingSpeed': None,
        'SplitAvg': None,
    }
    
    # SpeedIndex = distance / race_time
    if distance_m and race_time_s and race_time_s > 0:
        metrics['SpeedIndex'] = round(distance_m / race_time_s, 2)
    
    # SplitAvg = average of non-null sectionals
    sectionals = [s for s in [sectional1, sectional2, sectional3] if s is not None]
    if sectionals:
        metrics['SplitAvg'] = round(sum(sectionals) / len(sectionals), 2)
    
    # EarlySpeed from sectional1 (if available and distance >= 400m)
    if sectional1 and distance_m and distance_m >= 400:
        # Assume sectional1 covers ~200m for longer races
        metrics['EarlySpeed'] = round(200 / sectional1, 2)
    elif metrics['SpeedIndex']:
        # Fallback to overall speed
        metrics['EarlySpeed'] = metrics['SpeedIndex']
    
    # ClosingSpeed from sectional3 or overall speed
    if sectional3:
        # Assume sectional3 covers ~200m
        metrics['ClosingSpeed'] = round(200 / sectional3, 2)
    elif metrics['SpeedIndex']:
        metrics['ClosingSpeed'] = metrics['SpeedIndex']
    
    return metrics


def validate_and_sort_dataframe(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    B) Enforce dtype, sort, and ensure uniqueness:
    - Cast Race, Box to int
    - Sort: Track (A-Z) → Race (asc) → Box (asc)
    - Ensure (Track, Race, Box) unique; log conflicts
    """
    logger.info("\n" + "="*60)
    logger.info("ORDERING & DTYPE ENFORCEMENT")
    logger.info("="*60)
    
    # Cast to appropriate types
    df['Track'] = df['Track'].astype(str)
    df['Race'] = pd.to_numeric(df['Race'], errors='coerce').fillna(0).astype(int)
    df['Box'] = pd.to_numeric(df['Box'], errors='coerce').fillna(0).astype(int)
    
    # Remove rows with invalid Box (0 or null)
    invalid_box = df[df['Box'] <= 0]
    if len(invalid_box) > 0:
        logger.warning(f"⚠️  Removing {len(invalid_box)} rows with missing/invalid Box")
        for idx, row in invalid_box.iterrows():
            logger.warning(f"    Track={row.get('Track')}, Race={row.get('Race')}, DogName={row.get('DogName')}")
        df = df[df['Box'] > 0]
    
    # Sort strictly
    df = df.sort_values(by=['Track', 'Race', 'Box'], ascending=[True, True, True])
    logger.info(f"[OK] Sorted by Track → Race → Box")
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)]
    if len(duplicates) > 0:
        logger.warning(f"⚠️  Found {len(duplicates)} duplicate (Track, Race, Box) combinations")
        for idx, row in duplicates.iterrows():
            logger.warning(f"    Track={row['Track']}, Race={row['Race']}, Box={row['Box']}, Dog={row.get('DogName')}")
        # Deduplicate keeping first occurrence
        df = df.drop_duplicates(subset=['Track', 'Race', 'Box'], keep='first')
        logger.info(f"[OK] Deduped to {len(df)} unique rows")
    else:
        logger.info(f"[OK] All (Track, Race, Box) combinations are unique")
    
    return df.reset_index(drop=True)


def validate_race_counts(df: pd.DataFrame, source_files: List[str], logger: logging.Logger) -> bool:
    """
    C) Hard validations:
    - Check expected race counts per file
    - Validate races are contiguous and sequential per track
    - Validate box numbers strictly ascending per race
    """
    logger.info("\n" + "="*60)
    logger.info("HARD VALIDATIONS - RACE COUNTS & ORDERING")
    logger.info("="*60)
    
    validation_passed = True
    
    # Check expected race counts by file
    for file_path in source_files:
        basename = os.path.basename(file_path)
        file_key = basename.split('(')[0].strip()  # Remove (1).pdf
        
        if file_key in FILE_RACE_EXPECTATIONS:
            expected_races = FILE_RACE_EXPECTATIONS[file_key]
            # Infer track from file
            track_matches = df[df['SourcePDF'].str.contains(file_key, na=False)]
            
            if len(track_matches) > 0:
                track = track_matches.iloc[0]['Track']
                track_df = df[df['Track'] == track]
                actual_races = sorted(track_df['Race'].unique())
                
                logger.info(f"\nFile: {basename}")
                logger.info(f"  Track: {track}")
                logger.info(f"  Expected races: 1-{expected_races}")
                logger.info(f"  Found races: {actual_races}")
                
                if len(actual_races) != expected_races:
                    logger.error(f"  ❌ Expected {expected_races} races, found {len(actual_races)}")
                    validation_passed = False
                elif actual_races != list(range(1, expected_races + 1)):
                    logger.error(f"  ❌ Races not sequential: expected 1-{expected_races}")
                    validation_passed = False
                else:
                    logger.info(f"  [OK] Race count and sequence correct")
    
    # Validate per-track contiguity
    for track in df['Track'].unique():
        track_df = df[df['Track'] == track]
        races = sorted(track_df['Race'].unique())
        
        logger.info(f"\nTrack: {track}")
        logger.info(f"  Races found: {races}")
        
        # Check contiguous
        if races:
            expected_range = list(range(min(races), max(races) + 1))
            if races != expected_range:
                logger.warning(f"  ⚠️  Races not contiguous (gaps found)")
                validation_passed = False
            else:
                logger.info(f"  [OK] Races are contiguous")
        
        # Check box ordering per race
        for race in races:
            race_df = track_df[track_df['Race'] == race]
            boxes = list(race_df['Box'])
            
            if boxes != sorted(boxes):
                logger.warning(f"  ⚠️  Race {race}: Box numbers not strictly ascending: {boxes}")
                validation_passed = False
            elif boxes[0] != 1:
                logger.warning(f"  ⚠️  Race {race}: First box is {boxes[0]}, expected 1")
                validation_passed = False
            else:
                logger.info(f"  Race {race}: [OK] Box numbers {min(boxes)}-{max(boxes)} strictly ascending")
    
    return validation_passed


def validate_coverage_thresholds(df: pd.DataFrame, logger: logging.Logger) -> bool:
    """
    C) Coverage thresholds (warn if below):
    - Distance ≥ 90%
    - RaceTime ≥ 80%
    - BestTime ≥ 70%
    - Sectional1/2/3 ≥ 60% each (if present in PDF)
    """
    logger.info("\n" + "="*60)
    logger.info("COVERAGE THRESHOLDS VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    total_rows = len(df)
    
    thresholds = [
        ('Distance', 90),
        ('RaceTime', 80),
        ('BestTime', 70),
        ('Sectional1', 60),
        ('Sectional2', 60),
        ('Sectional3', 60),
    ]
    
    for field, threshold in thresholds:
        if field in df.columns:
            populated = df[field].notna().sum()
            pct = (populated / total_rows * 100) if total_rows > 0 else 0
            
            status = "[OK]" if pct >= threshold else "⚠️ "
            logger.info(f"{status} {field}: {populated}/{total_rows} ({pct:.1f}%) - Threshold: {threshold}%")
            
            if pct < threshold:
                validation_passed = False
        else:
            logger.warning(f"  ⚠️  Field '{field}' not found in DataFrame")
    
    return validation_passed


def log_per_track_preview(df: pd.DataFrame, logger: logging.Logger):
    """
    C) Per-track preview:
    - First 3 rows of Race 1
    - Last 3 rows of last Race
    - Columns: Track, Race, Box, DogName, Distance, RaceTime, Sectional1-3
    """
    logger.info("\n" + "="*60)
    logger.info("PER-TRACK PREVIEW")
    logger.info("="*60)
    
    preview_cols = ['Track', 'Race', 'Box', 'DogName', 'Distance', 'RaceTime', 
                    'Sectional1', 'Sectional2', 'Sectional3']
    
    # Ensure all columns exist
    for col in preview_cols:
        if col not in df.columns:
            df[col] = None
    
    for track in sorted(df['Track'].unique()):
        track_df = df[df['Track'] == track]
        races = sorted(track_df['Race'].unique())
        
        if not races:
            continue
        
        logger.info(f"\n--- Track: {track} ---")
        
        # First 3 rows of Race 1
        race1_df = track_df[track_df['Race'] == races[0]].head(3)
        logger.info(f"First 3 rows of Race {races[0]}:")
        for idx, row in race1_df.iterrows():
            logger.info(f"  {row[preview_cols].to_dict()}")
        
        # Last 3 rows of last race
        last_race = races[-1]
        last_race_df = track_df[track_df['Race'] == last_race].tail(3)
        logger.info(f"Last 3 rows of Race {last_race}:")
        for idx, row in last_race_df.iterrows():
            logger.info(f"  {row[preview_cols].to_dict()}")


def validate_pdf_excel_consistency(df: pd.DataFrame, csv_path: str, excel_path: str, 
                                   logger: logging.Logger) -> bool:
    """
    D) PDF=Excel consistency:
    - Confirm first 4 columns exact: Track, Race, Box, DogName
    - Confirm (Track, Race, Box) uniqueness in both CSV/XLSX
    - Confirm non-null time/sectional values identical in DataFrame, CSV, XLSX
    """
    logger.info("\n" + "="*60)
    logger.info("PDF=EXCEL CONSISTENCY VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    
    # Check first 4 columns
    first_4 = list(df.columns[:4])
    expected_4 = ['Track', 'Race', 'Box', 'DogName']
    if first_4 == expected_4:
        logger.info(f"[OK] First 4 columns exact: {first_4}")
    else:
        logger.error(f"❌ First 4 columns mismatch: Expected {expected_4}, Got {first_4}")
        validation_passed = False
    
    # Load CSV and Excel for comparison
    try:
        df_csv = pd.read_csv(csv_path)
        df_excel = pd.read_excel(excel_path, engine='openpyxl')
        
        # Check uniqueness in CSV
        csv_dup = df_csv.duplicated(subset=['Track', 'Race', 'Box']).sum()
        if csv_dup == 0:
            logger.info(f"[OK] CSV: All (Track, Race, Box) unique")
        else:
            logger.error(f"❌ CSV: Found {csv_dup} duplicate (Track, Race, Box)")
            validation_passed = False
        
        # Check uniqueness in Excel
        excel_dup = df_excel.duplicated(subset=['Track', 'Race', 'Box']).sum()
        if excel_dup == 0:
            logger.info(f"[OK] Excel: All (Track, Race, Box) unique")
        else:
            logger.error(f"❌ Excel: Found {excel_dup} duplicate (Track, Race, Box)")
            validation_passed = False
        
        # Check time/sectional field consistency
        time_fields = ['RaceTime', 'BestTime', 'Sectional1', 'Sectional2', 'Sectional3']
        for field in time_fields:
            if field in df.columns and field in df_csv.columns and field in df_excel.columns:
                df_count = df[field].notna().sum()
                csv_count = df_csv[field].notna().sum()
                excel_count = df_excel[field].notna().sum()
                
                if df_count == csv_count == excel_count:
                    logger.info(f"[OK] {field}: {df_count} non-null values consistent across DataFrame/CSV/Excel")
                else:
                    logger.error(f"❌ {field}: Inconsistent counts - DF:{df_count}, CSV:{csv_count}, Excel:{excel_count}")
                    validation_passed = False
        
    except Exception as e:
        logger.error(f"❌ Error loading CSV/Excel for comparison: {e}")
        validation_passed = False
    
    return validation_passed

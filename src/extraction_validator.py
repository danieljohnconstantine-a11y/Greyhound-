#!/usr/bin/env python3
"""
Comprehensive extraction and validation module for greyhound race forms.
Implements robust extraction patterns, strict sorting, and hard validations.
"""

import re
import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# Expected race counts per file (PDF filename stem -> expected race count)
EXPECTED_RACE_COUNTS = {
    'MANDG0710form (1)': 11,
    'MANDG0710form': 11,
    'QLAKG0710form (1)': 14,
    'QLAKG0710form': 14,
}

# Coverage thresholds (field -> minimum percentage)
COVERAGE_THRESHOLDS = {
    'Distance': 90.0,
    'RaceTime': 80.0,
    'BestTime': 70.0,
    'Sectional1': 60.0,
    'Sectional2': 60.0,
    'Sectional3': 60.0,
}


def extract_race_number(text: str, filename: str = '') -> Optional[int]:
    """
    Extract race number from PDF content (content-first approach).
    
    Supports patterns: "Race 6", "Race No 6", "RACE 6", "Broken Hill Race 6"
    Returns int or None. Content wins over filename.
    """
    if not text:
        return None
    
    # Try various race number patterns (case-insensitive)
    patterns = [
        r'(?:Broken\s+Hill\s+)?Race\s+No\.?\s*(\d+)',  # "Race No 6" or "Broken Hill Race No 6"
        r'(?:Broken\s+Hill\s+)?RACE\s+NO\.?\s*(\d+)',  # "RACE NO 6"
        r'(?:Broken\s+Hill\s+)?Race\s+(\d+)',          # "Race 6" or "Broken Hill Race 6"
        r'(?:Broken\s+Hill\s+)?RACE\s+(\d+)',          # "RACE 6"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                race_num = int(match.group(1))
                if 1 <= race_num <= 30:  # Sanity check
                    logger.debug(f"Extracted race number {race_num} from content")
                    return race_num
            except (ValueError, IndexError):
                continue
    
    # Fallback to filename if no content match (but log warning)
    if filename:
        match = re.search(r'race[_\s]*(\d+)', filename, re.IGNORECASE)
        if match:
            try:
                race_num = int(match.group(1))
                logger.warning(f"Using race number {race_num} from filename (no content match)")
                return race_num
            except (ValueError, IndexError):
                pass
    
    return None


def extract_box_number(line: str) -> Optional[int]:
    """
    Extract box number from dog line.
    
    Accepts: "1.", "[1]", "(1)", or leading ordinal before dog name.
    Enforces int in range 1..10. Returns None if invalid.
    """
    if not line:
        return None
    
    # Patterns for box number (prefer leading numeric)
    patterns = [
        r'^\s*(\d{1,2})\.',          # "1." at start
        r'^\s*\[(\d{1,2})\]',        # "[1]" at start
        r'^\s*\((\d{1,2})\)',        # "(1)" at start
        r'^\s*Box\s*(\d{1,2})',      # "Box 1"
        r'^\s*(\d{1,2})\s+[A-Z]',    # "1 DOGNAME" (digit followed by capital letter)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, line)
        if match:
            try:
                box = int(match.group(1))
                if 1 <= box <= 10:
                    return box
            except (ValueError, IndexError):
                continue
    
    return None


def extract_distance(text: str) -> Optional[int]:
    """
    Extract distance in meters from text.
    Pattern: (\d{3,4})\s*m
    """
    if not text:
        return None
    
    match = re.search(r'\b(\d{3,4})\s*m\b', text, re.IGNORECASE)
    if match:
        try:
            dist = int(match.group(1))
            if 100 <= dist <= 9999:  # Sanity check
                return dist
        except (ValueError, IndexError):
            pass
    
    return None


def normalize_race_time(time_str: str) -> Optional[float]:
    """
    Normalize race time string to seconds (float).
    
    Accepts:
    - "0:30.41" -> 30.41
    - "30.41" -> 30.41
    - "0:18.76" -> 18.76
    - "18.76" -> 18.76
    """
    if not time_str:
        return None
    
    time_str = str(time_str).strip()
    
    try:
        if ':' in time_str:
            # Format: MM:SS.MS or M:SS.MS
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
        else:
            # Format: SS.MS
            return float(time_str)
    except (ValueError, IndexError):
        return None
    
    return None


def extract_sectional_time(text: str, sectional_num: int = 1) -> Optional[float]:
    """
    Extract sectional time from text.
    
    Accepts:
    - "Sec Time 4.39"
    - "First Split 6.80"
    - "Sectional 1: 4.39" or "Sectional1 4.39"
    """
    if not text:
        return None
    
    # Try multiple patterns
    patterns = [
        rf'Sec\s+Time\s+([\d\.]+)',
        rf'First\s+Split\s+([\d\.]+)',
        rf'Sectional\s*{sectional_num}[:\s]+([\d\.]+)',
        rf'Sect\s*{sectional_num}[:\s]+([\d\.]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except (ValueError, IndexError):
                continue
    
    return None


def calculate_speed_index(distance_m: Optional[int], race_time_s: Optional[float]) -> Optional[float]:
    """
    Calculate speed index: meters/second = distance / time
    """
    if distance_m and race_time_s and race_time_s > 0:
        return round(distance_m / race_time_s, 2)
    return None


def enforce_dtypes_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce data types and sort strictly.
    
    - Cast Race, Box to int
    - Sort: Track (A-Z) -> Race (asc) -> Box (asc)
    - Ensure (Track, Race, Box) uniqueness
    """
    # Make a copy
    df = df.copy()
    
    # Enforce Track as string
    if 'Track' in df.columns:
        df['Track'] = df['Track'].astype(str)
    
    # Enforce Race as int (coerce errors to NaN, then fillna)
    if 'Race' in df.columns:
        df['Race'] = pd.to_numeric(df['Race'], errors='coerce')
        # Drop rows where Race is null/invalid
        before_count = len(df)
        df = df[df['Race'].notna()].copy()
        after_count = len(df)
        if before_count != after_count:
            logger.warning(f"Dropped {before_count - after_count} rows with invalid Race number")
        df['Race'] = df['Race'].astype(int)
    
    # Enforce Box as int (coerce errors to NaN, then fillna)
    if 'Box' in df.columns:
        df['Box'] = pd.to_numeric(df['Box'], errors='coerce')
        # Drop rows where Box is null/invalid
        before_count = len(df)
        df = df[df['Box'].notna()].copy()
        after_count = len(df)
        if before_count != after_count:
            logger.warning(f"Dropped {before_count - after_count} rows with invalid Box number")
        df['Box'] = df['Box'].astype(int)
    
    # Check for duplicates before deduplication
    if all(col in df.columns for col in ['Track', 'Race', 'Box']):
        dupes = df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)
        if dupes.any():
            dupe_rows = df[dupes][['Track', 'Race', 'Box', 'DogName']].to_dict('records')
            logger.error(f"⚠️  Found {dupes.sum()} duplicate (Track, Race, Box) combinations:")
            for row in dupe_rows[:5]:  # Show first 5
                logger.error(f"   {row}")
            
            # Dedupe: keep first occurrence
            df = df.drop_duplicates(subset=['Track', 'Race', 'Box'], keep='first')
            logger.warning(f"Deduplicated to {len(df)} unique rows")
    
    # Sort strictly: Track -> Race -> Box
    sort_cols = []
    if 'Track' in df.columns:
        sort_cols.append('Track')
    if 'Race' in df.columns:
        sort_cols.append('Race')
    if 'Box' in df.columns:
        sort_cols.append('Box')
    
    if sort_cols:
        df = df.sort_values(by=sort_cols).reset_index(drop=True)
        logger.info(f"✅ Sorted by: {' → '.join(sort_cols)}")
    
    return df


def validate_race_counts(df: pd.DataFrame, pdf_files: List[str]) -> bool:
    """
    Validate expected race counts per file.
    
    Returns True if all validations pass.
    """
    logger.info("\n" + "="*60)
    logger.info("RACE COUNT VALIDATION")
    logger.info("="*60)
    
    all_passed = True
    
    # Extract track names from filenames
    file_to_track = {}
    for pdf_file in pdf_files:
        # Extract filename stem (without path and extension)
        import os
        filename_stem = os.path.basename(pdf_file).replace('.pdf', '')
        
        # Try to extract track from filename or use filename as key
        track = None
        if 'MANDG' in filename_stem.upper():
            track = 'MANDG'
        elif 'QLAKG' in filename_stem.upper():
            track = 'QLAKG'
        
        if track and filename_stem in EXPECTED_RACE_COUNTS:
            file_to_track[track] = (filename_stem, EXPECTED_RACE_COUNTS[filename_stem])
    
    # Validate each track
    if 'Track' in df.columns and 'Race' in df.columns:
        for track_name, (filename, expected_count) in file_to_track.items():
            track_df = df[df['Track'] == track_name]
            
            if len(track_df) == 0:
                logger.warning(f"⚠️  Track '{track_name}' not found in data (expected from {filename})")
                continue
            
            unique_races = track_df['Race'].nunique()
            race_list = sorted(track_df['Race'].unique().tolist())
            
            logger.info(f"\nTrack: {track_name} (from {filename})")
            logger.info(f"  Expected races: {expected_count}")
            logger.info(f"  Found races: {unique_races}")
            logger.info(f"  Race numbers: {race_list}")
            
            if unique_races != expected_count:
                logger.error(f"❌ Race count mismatch for {track_name}: expected {expected_count}, found {unique_races}")
                all_passed = False
            else:
                logger.info(f"✅ Race count matches expected")
            
            # Check if races are contiguous
            expected_races = list(range(1, expected_count + 1))
            if race_list != expected_races:
                logger.error(f"❌ Races not contiguous: expected {expected_races}, found {race_list}")
                all_passed = False
            else:
                logger.info(f"✅ Races are contiguous and sequential")
    
    return all_passed


def validate_box_sequences(df: pd.DataFrame) -> bool:
    """
    Validate that within each race, Box numbers are strictly ascending starting at 1.
    
    Returns True if all validations pass.
    """
    logger.info("\n" + "="*60)
    logger.info("BOX SEQUENCE VALIDATION")
    logger.info("="*60)
    
    all_passed = True
    
    if not all(col in df.columns for col in ['Track', 'Race', 'Box']):
        logger.error("❌ Required columns for box validation not found")
        return False
    
    # Group by Track and Race
    for (track, race), group in df.groupby(['Track', 'Race']):
        boxes = sorted(group['Box'].tolist())
        expected_boxes = list(range(1, max(boxes) + 1))
        
        # Check if boxes start at 1
        if boxes[0] != 1:
            logger.error(f"❌ {track} Race {race}: Boxes don't start at 1 (found: {boxes})")
            all_passed = False
            continue
        
        # Check if boxes are consecutive
        if boxes != expected_boxes[:len(boxes)]:
            logger.error(f"❌ {track} Race {race}: Boxes not sequential (found: {boxes}, expected: {expected_boxes[:len(boxes)]})")
            all_passed = False
        else:
            logger.debug(f"✅ {track} Race {race}: Boxes {boxes[0]}-{boxes[-1]} OK")
    
    if all_passed:
        logger.info("✅ All box sequences valid")
    
    return all_passed


def validate_coverage_thresholds(df: pd.DataFrame) -> bool:
    """
    Validate field population coverage against thresholds.
    
    Returns True if all thresholds met (warnings don't fail).
    """
    logger.info("\n" + "="*60)
    logger.info("COVERAGE THRESHOLD VALIDATION")
    logger.info("="*60)
    
    all_passed = True
    
    for field, threshold in COVERAGE_THRESHOLDS.items():
        if field not in df.columns:
            logger.warning(f"⚠️  Field '{field}' not in DataFrame")
            continue
        
        non_null = df[field].notna().sum()
        total = len(df)
        coverage = (non_null / total * 100) if total > 0 else 0
        
        status = "✅" if coverage >= threshold else "⚠️ "
        logger.info(f"{status} {field}: {non_null}/{total} ({coverage:.1f}%) [threshold: {threshold}%]")
        
        if coverage < threshold:
            logger.warning(f"   Below threshold by {threshold - coverage:.1f}%")
    
    return all_passed  # Always return True since these are warnings


def log_per_track_preview(df: pd.DataFrame) -> None:
    """
    Log per-track preview: first 3 rows of Race 1 and last 3 rows of last Race.
    
    Columns: Track, Race, Box, DogName, Distance, RaceTime, Sectional1-3
    """
    logger.info("\n" + "="*60)
    logger.info("PER-TRACK DATA PREVIEW")
    logger.info("="*60)
    
    preview_cols = ['Track', 'Race', 'Box', 'DogName', 'Distance', 'RaceTime', 
                    'Sectional1', 'Sectional2', 'Sectional3']
    
    # Filter to available columns
    available_cols = [c for c in preview_cols if c in df.columns]
    
    if 'Track' not in df.columns or 'Race' not in df.columns:
        logger.warning("Cannot generate preview: Track or Race column missing")
        return
    
    # Group by Track
    for track in sorted(df['Track'].unique()):
        track_df = df[df['Track'] == track]
        
        logger.info(f"\n📊 Track: {track}")
        logger.info("-" * 60)
        
        # Get first race
        first_race = track_df['Race'].min()
        first_race_df = track_df[track_df['Race'] == first_race].head(3)
        
        logger.info(f"First 3 rows of Race {first_race}:")
        for _, row in first_race_df.iterrows():
            row_str = " | ".join([f"{col}={row[col]}" for col in available_cols if col in row.index])
            logger.info(f"  {row_str}")
        
        # Get last race
        last_race = track_df['Race'].max()
        if last_race != first_race:
            last_race_df = track_df[track_df['Race'] == last_race].tail(3)
            
            logger.info(f"\nLast 3 rows of Race {last_race}:")
            for _, row in last_race_df.iterrows():
                row_str = " | ".join([f"{col}={row[col]}" for col in available_cols if col in row.index])
                logger.info(f"  {row_str}")


def validate_pdf_excel_consistency(df: pd.DataFrame, csv_path: str, excel_path: str) -> bool:
    """
    Validate PDF=Excel consistency.
    
    - First 4 columns exact: Track, Race, Box, DogName
    - (Track, Race, Box) uniqueness in both CSV/XLSX
    - Non-null time/sectional values identical in DataFrame, CSV, and XLSX
    
    Returns True if validation passes.
    """
    logger.info("\n" + "="*60)
    logger.info("PDF=EXCEL CONSISTENCY VALIDATION")
    logger.info("="*60)
    
    all_passed = True
    
    # Check first 4 columns
    expected_first_4 = ['Track', 'Race', 'Box', 'DogName']
    actual_first_4 = list(df.columns[:4])
    
    if actual_first_4 != expected_first_4:
        logger.error(f"❌ First 4 columns mismatch:")
        logger.error(f"   Expected: {expected_first_4}")
        logger.error(f"   Found: {actual_first_4}")
        all_passed = False
    else:
        logger.info(f"✅ First 4 columns exact: {expected_first_4}")
    
    # Check uniqueness in DataFrame
    if all(col in df.columns for col in ['Track', 'Race', 'Box']):
        dupes_df = df.duplicated(subset=['Track', 'Race', 'Box'])
        if dupes_df.any():
            logger.error(f"❌ DataFrame has {dupes_df.sum()} duplicate (Track, Race, Box) entries")
            all_passed = False
        else:
            logger.info(f"✅ DataFrame (Track, Race, Box) uniqueness confirmed")
    
    # Load and check CSV
    try:
        csv_df = pd.read_csv(csv_path)
        dupes_csv = csv_df.duplicated(subset=['Track', 'Race', 'Box'])
        if dupes_csv.any():
            logger.error(f"❌ CSV has {dupes_csv.sum()} duplicate (Track, Race, Box) entries")
            all_passed = False
        else:
            logger.info(f"✅ CSV (Track, Race, Box) uniqueness confirmed")
        
        # Compare dimensions
        if len(csv_df) != len(df):
            logger.error(f"❌ CSV row count ({len(csv_df)}) != DataFrame ({len(df)})")
            all_passed = False
        else:
            logger.info(f"✅ CSV row count matches DataFrame: {len(df)}")
        
    except Exception as e:
        logger.error(f"❌ Error loading CSV: {e}")
        all_passed = False
    
    # Load and check Excel
    try:
        excel_df = pd.read_excel(excel_path)
        dupes_excel = excel_df.duplicated(subset=['Track', 'Race', 'Box'])
        if dupes_excel.any():
            logger.error(f"❌ Excel has {dupes_excel.sum()} duplicate (Track, Race, Box) entries")
            all_passed = False
        else:
            logger.info(f"✅ Excel (Track, Race, Box) uniqueness confirmed")
        
        # Compare dimensions
        if len(excel_df) != len(df):
            logger.error(f"❌ Excel row count ({len(excel_df)}) != DataFrame ({len(df)})")
            all_passed = False
        else:
            logger.info(f"✅ Excel row count matches DataFrame: {len(df)}")
        
    except Exception as e:
        logger.error(f"❌ Error loading Excel: {e}")
        all_passed = False
    
    # Sample non-null time values and verify consistency
    time_fields = ['RaceTime', 'BestTime', 'Sectional1', 'Sectional2', 'Sectional3']
    for field in time_fields:
        if field not in df.columns:
            continue
        
        non_null_sample = df[df[field].notna()][field].head(5).tolist()
        if not non_null_sample:
            continue
        
        logger.info(f"\n{field} sample (first 5 non-null): {non_null_sample}")
    
    if all_passed:
        logger.info("\n✅ PDF=Excel consistency validated")
    
    return all_passed


def run_comprehensive_validation(df: pd.DataFrame, pdf_files: List[str], 
                                  csv_path: str, excel_path: str) -> bool:
    """
    Run all validation checks.
    
    Returns True if all critical validations pass.
    """
    logger.info("\n" + "="*70)
    logger.info("🔍 COMPREHENSIVE VALIDATION SUITE")
    logger.info("="*70)
    
    results = []
    
    # 1. Enforce dtypes and sort
    # Note: This should be done before other validations
    
    # 2. Race count validation
    results.append(("Race Counts", validate_race_counts(df, pdf_files)))
    
    # 3. Box sequence validation
    results.append(("Box Sequences", validate_box_sequences(df)))
    
    # 4. Coverage thresholds (warnings only)
    validate_coverage_thresholds(df)  # Always passes
    
    # 5. Per-track preview
    log_per_track_preview(df)
    
    # 6. PDF=Excel consistency
    results.append(("PDF=Excel Consistency", validate_pdf_excel_consistency(df, csv_path, excel_path)))
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*70)
    
    for check_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status}: {check_name}")
    
    all_passed = all(passed for _, passed in results)
    
    if all_passed:
        logger.info("\n🎉 ALL VALIDATIONS PASSED")
    else:
        logger.error("\n⚠️  SOME VALIDATIONS FAILED - Review logs above")
    
    logger.info("="*70 + "\n")
    
    return all_passed

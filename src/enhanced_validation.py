"""
Enhanced validation module for greyhound race form parsing
Implements comprehensive validation rules for extraction accuracy and data consistency
"""

import logging
import pandas as pd
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

# Expected race counts per file
EXPECTED_RACE_COUNTS = {
    "MANDG0710form (1).pdf": {"track": "MANDG", "races": 11, "range": (1, 11)},
    "QLAKG0710form (1).pdf": {"track": "QLAKG", "races": 14, "range": (1, 14)},
}

# Coverage thresholds
COVERAGE_THRESHOLDS = {
    "Distance": 0.90,  # 90%
    "RaceTime": 0.80,  # 80%
    "BestTime": 0.70,  # 70%
    "Sectional1": 0.60,  # 60%
    "Sectional2": 0.60,  # 60%
    "Sectional3": 0.60,  # 60%
}


def validate_race_counts(df: pd.DataFrame, source_file: str) -> Tuple[bool, List[str]]:
    """
    Validate that race counts match expected values per file
    
    Returns:
        (is_valid, error_messages)
    """
    errors = []
    
    if source_file in EXPECTED_RACE_COUNTS:
        expected = EXPECTED_RACE_COUNTS[source_file]
        expected_track = expected["track"]
        expected_count = expected["races"]
        expected_range = expected["range"]
        
        # Filter to this track
        track_df = df[df['Track'] == expected_track]
        
        if track_df.empty:
            errors.append(f"❌ {source_file}: Expected track '{expected_track}' not found in data")
            return False, errors
        
        # Check unique race numbers
        unique_races = sorted(track_df['Race'].dropna().unique())
        actual_count = len(unique_races)
        
        if actual_count != expected_count:
            errors.append(
                f"❌ {source_file}: Expected {expected_count} races, found {actual_count} (races: {unique_races})"
            )
            return False, errors
        
        # Check race range is continuous
        if unique_races != list(range(expected_range[0], expected_range[1] + 1)):
            errors.append(
                f"❌ {source_file}: Races not continuous. Expected {list(range(expected_range[0], expected_range[1] + 1))}, got {unique_races}"
            )
            return False, errors
        
        logger.info(f"✅ {source_file}: Race count validated ({actual_count} races: {expected_range[0]}-{expected_range[1]})")
    
    return True, errors


def validate_race_box_ordering(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that races are sequential and boxes within each race are strictly ascending
    
    Returns:
        (is_valid, error_messages)
    """
    errors = []
    
    for track in df['Track'].unique():
        track_df = df[df['Track'] == track].copy()
        races = sorted(track_df['Race'].dropna().unique())
        
        # Check races are sequential (no interleaving)
        for i, race in enumerate(races):
            race_df = track_df[track_df['Race'] == race]
            boxes = race_df['Box'].tolist()
            
            # Check boxes start at 1 and are strictly ascending
            if boxes:
                if boxes[0] != 1:
                    errors.append(f"❌ Track {track}, Race {race}: First box is {boxes[0]}, expected 1")
                
                # Check strictly ascending
                for j in range(len(boxes) - 1):
                    if boxes[j] >= boxes[j+1]:
                        errors.append(
                            f"❌ Track {track}, Race {race}: Box ordering not strictly ascending at positions {j},{j+1}: {boxes[j]}, {boxes[j+1]}"
                        )
                        break
    
    if not errors:
        logger.info("✅ Race/Box ordering validated: all races sequential, boxes strictly ascending")
    
    return len(errors) == 0, errors


def validate_coverage_thresholds(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that key fields meet minimum population thresholds
    
    Returns:
        (is_valid, warnings)
    """
    warnings = []
    total_rows = len(df)
    
    for field, threshold in COVERAGE_THRESHOLDS.items():
        if field in df.columns:
            non_null = df[field].notna().sum()
            coverage = non_null / total_rows if total_rows > 0 else 0
            
            if coverage < threshold:
                warnings.append(
                    f"⚠️  {field}: {coverage*100:.1f}% populated (threshold: {threshold*100:.0f}%) - {non_null}/{total_rows} rows"
                )
            else:
                logger.info(f"✅ {field}: {coverage*100:.1f}% populated ({non_null}/{total_rows} rows)")
    
    return len(warnings) == 0, warnings


def validate_uniqueness(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that (Track, Race, Box) combinations are unique
    
    Returns:
        (is_valid, error_messages)
    """
    errors = []
    
    duplicates = df[df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)]
    
    if not duplicates.empty:
        dup_count = len(duplicates)
        errors.append(f"❌ Found {dup_count} duplicate (Track, Race, Box) combinations:")
        
        # Show first 5 duplicates
        for idx, row in duplicates.head(10).iterrows():
            errors.append(f"   - Track={row['Track']}, Race={row['Race']}, Box={row['Box']}, Dog={row['DogName']}")
    else:
        logger.info("✅ No duplicate (Track, Race, Box) combinations found")
    
    return len(errors) == 0, errors


def log_track_preview(df: pd.DataFrame):
    """
    Log preview of first 3 and last 3 rows per track showing key timing fields
    """
    preview_columns = ['Track', 'Race', 'Box', 'DogName', 'Distance', 'BestTime', 'Sectional1', 'Sectional2', 'Sectional3']
    
    logger.info("\n" + "="*100)
    logger.info("TRACK PREVIEW - First 3 and Last 3 rows per track")
    logger.info("="*100)
    
    for track in sorted(df['Track'].unique()):
        track_df = df[df['Track'] == track].copy()
        
        if track_df.empty:
            continue
        
        races = sorted(track_df['Race'].unique())
        first_race = races[0]
        last_race = races[-1]
        
        logger.info(f"\n{'─'*100}")
        logger.info(f"Track: {track} | Total Races: {len(races)} | Range: {first_race}-{last_race}")
        logger.info(f"{'─'*100}")
        
        # First 3 rows of first race
        first_race_df = track_df[track_df['Race'] == first_race].head(3)
        logger.info(f"\nFirst 3 rows of Race {first_race}:")
        for col in preview_columns:
            if col in first_race_df.columns:
                logger.info(f"  {col:15s}: {first_race_df[col].tolist()}")
        
        # Last 3 rows of last race
        last_race_df = track_df[track_df['Race'] == last_race].tail(3)
        logger.info(f"\nLast 3 rows of Race {last_race}:")
        for col in preview_columns:
            if col in last_race_df.columns:
                logger.info(f"  {col:15s}: {last_race_df[col].tolist()}")
    
    logger.info("\n" + "="*100)


def validate_pdf_excel_consistency(df: pd.DataFrame, csv_path: str, xlsx_path: str) -> Tuple[bool, List[str]]:
    """
    Validate that CSV and Excel outputs are consistent with DataFrame
    
    Returns:
        (is_valid, error_messages)
    """
    errors = []
    
    try:
        # Read back the files
        csv_df = pd.read_csv(csv_path)
        xlsx_df = pd.read_excel(xlsx_path)
        
        # Check column order (first 4 must be exact)
        expected_first_4 = ['Track', 'Race', 'Box', 'DogName']
        
        for idx, col in enumerate(expected_first_4):
            if csv_df.columns[idx] != col:
                errors.append(f"❌ CSV: Column {idx} is '{csv_df.columns[idx]}', expected '{col}'")
            if xlsx_df.columns[idx] != col:
                errors.append(f"❌ XLSX: Column {idx} is '{xlsx_df.columns[idx]}', expected '{col}'")
        
        # Check row counts match
        if len(csv_df) != len(df):
            errors.append(f"❌ CSV row count mismatch: DataFrame={len(df)}, CSV={len(csv_df)}")
        if len(xlsx_df) != len(df):
            errors.append(f"❌ XLSX row count mismatch: DataFrame={len(df)}, XLSX={len(xlsx_df)}")
        
        # Check uniqueness in outputs
        csv_dups = csv_df[csv_df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)]
        if not csv_dups.empty:
            errors.append(f"❌ CSV contains {len(csv_dups)} duplicate (Track, Race, Box) rows")
        
        xlsx_dups = xlsx_df[xlsx_df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)]
        if not xlsx_dups.empty:
            errors.append(f"❌ XLSX contains {len(xlsx_dups)} duplicate (Track, Race, Box) rows")
        
        # Sample check: verify timing values match
        timing_cols = ['BestTime', 'Sectional1', 'Sectional2', 'Sectional3', 'SpeedIndex']
        for col in timing_cols:
            if col in df.columns and col in csv_df.columns and col in xlsx_df.columns:
                df_non_null = df[col].notna().sum()
                csv_non_null = csv_df[col].notna().sum()
                xlsx_non_null = xlsx_df[col].notna().sum()
                
                if df_non_null != csv_non_null:
                    errors.append(f"❌ {col}: DataFrame has {df_non_null} non-null, CSV has {csv_non_null}")
                if df_non_null != xlsx_non_null:
                    errors.append(f"❌ {col}: DataFrame has {df_non_null} non-null, XLSX has {xlsx_non_null}")
        
        if not errors:
            logger.info("✅ PDF=Excel consistency validated: CSV and XLSX match DataFrame")
    
    except Exception as e:
        errors.append(f"❌ Failed to validate file consistency: {e}")
    
    return len(errors) == 0, errors


def run_comprehensive_validation(df: pd.DataFrame, source_files: List[str], 
                                 csv_path: Optional[str] = None, 
                                 xlsx_path: Optional[str] = None) -> bool:
    """
    Run all validation checks and return overall status
    
    Returns:
        True if all validations pass, False otherwise
    """
    logger.info("\n" + "="*100)
    logger.info("COMPREHENSIVE VALIDATION REPORT")
    logger.info("="*100)
    
    all_valid = True
    all_messages = []
    
    # 1. Race count validation
    logger.info("\n1. RACE COUNT VALIDATION")
    logger.info("-" * 50)
    for source_file in source_files:
        valid, errors = validate_race_counts(df, source_file)
        if not valid:
            all_valid = False
            all_messages.extend(errors)
        for error in errors:
            logger.error(error)
    
    # 2. Race/Box ordering validation
    logger.info("\n2. RACE/BOX ORDERING VALIDATION")
    logger.info("-" * 50)
    valid, errors = validate_race_box_ordering(df)
    if not valid:
        all_valid = False
        all_messages.extend(errors)
    for error in errors:
        logger.error(error)
    
    # 3. Uniqueness validation
    logger.info("\n3. UNIQUENESS VALIDATION")
    logger.info("-" * 50)
    valid, errors = validate_uniqueness(df)
    if not valid:
        all_valid = False
        all_messages.extend(errors)
    for error in errors:
        logger.error(error)
    
    # 4. Coverage threshold validation
    logger.info("\n4. COVERAGE THRESHOLD VALIDATION")
    logger.info("-" * 50)
    valid, warnings = validate_coverage_thresholds(df)
    if not valid:
        for warning in warnings:
            logger.warning(warning)
        all_messages.extend(warnings)
    
    # 5. Track preview
    logger.info("\n5. TRACK PREVIEW")
    log_track_preview(df)
    
    # 6. PDF=Excel consistency (if paths provided)
    if csv_path and xlsx_path:
        logger.info("\n6. PDF=EXCEL CONSISTENCY VALIDATION")
        logger.info("-" * 50)
        valid, errors = validate_pdf_excel_consistency(df, csv_path, xlsx_path)
        if not valid:
            all_valid = False
            all_messages.extend(errors)
        for error in errors:
            logger.error(error)
    
    # Summary
    logger.info("\n" + "="*100)
    if all_valid:
        logger.info("✅ ALL VALIDATIONS PASSED")
    else:
        logger.error("❌ VALIDATION FAILURES DETECTED")
        logger.error(f"Total issues: {len([m for m in all_messages if '❌' in m])}")
        logger.warning(f"Total warnings: {len([m for m in all_messages if '⚠️' in m])}")
    logger.info("="*100 + "\n")
    
    return all_valid

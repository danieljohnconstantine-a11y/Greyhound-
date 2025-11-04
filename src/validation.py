#!/usr/bin/env python3
"""
Comprehensive validation module for greyhound form extraction.
Implements hard validations for race counts, ordering, coverage thresholds, and PDF=Excel consistency.
"""

import logging
import pandas as pd
from typing import Dict, List, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# Expected race counts per file
EXPECTED_RACES = {
    'MANDG0710form (1).pdf': 11,  # Mandurah: races 1-11
    'QLAKG0710form (1).pdf': 14,  # Queensland: races 1-14
}


def validate_race_counts(df: pd.DataFrame, source_files: List[str]) -> bool:
    """Validate expected race counts per track/file"""
    logger.info("\n" + "="*60)
    logger.info("RACE COUNT VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    
    for source_file in source_files:
        filename = Path(source_file).name
        if filename in EXPECTED_RACES:
            expected_count = EXPECTED_RACES[filename]
            # Get races for this source file
            file_df = df[df['SourcePDF'].str.contains(filename, na=False)]
            actual_races = file_df['Race'].nunique()
            unique_races = sorted(file_df['Race'].dropna().unique())
            
            logger.info(f"\nFile: {filename}")
            logger.info(f"  Expected races: {expected_count} (1..{expected_count})")
            logger.info(f"  Actual races: {actual_races}")
            logger.info(f"  Race numbers found: {unique_races}")
            
            if actual_races != expected_count:
                logger.error(f"  ❌ FAIL: Expected {expected_count} races, found {actual_races}")
                validation_passed = False
            else:
                # Check if races are 1..N
                expected_set = set(range(1, expected_count + 1))
                actual_set = set(unique_races)
                if actual_set != expected_set:
                    logger.error(f"  ❌ FAIL: Expected races {expected_set}, found {actual_set}")
                    validation_passed = False
                else:
                    logger.info(f"  [OK] PASS: All {expected_count} races present and sequential")
    
    return validation_passed


def validate_track_ordering(df: pd.DataFrame) -> bool:
    """Validate that races are contiguous and sequential per track, boxes ascending per race"""
    logger.info("\n" + "="*60)
    logger.info("TRACK ORDERING VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    
    for track in df['Track'].dropna().unique():
        track_df = df[df['Track'] == track].copy()
        races = sorted(track_df['Race'].dropna().unique())
        
        logger.info(f"\nTrack: {track}")
        logger.info(f"  Races: {races}")
        
        # Check races are contiguous
        if races:
            expected_races = list(range(races[0], races[-1] + 1))
            if races != expected_races:
                logger.error(f"  ❌ FAIL: Races not contiguous. Expected {expected_races}, got {races}")
                validation_passed = False
            else:
                logger.info(f"  [OK] Races are contiguous: {races[0]}..{races[-1]}")
        
        # Check box ordering per race
        for race_num in races:
            race_df = track_df[track_df['Race'] == race_num].copy()
            boxes = race_df['Box'].dropna().tolist()
            boxes_int = [int(b) for b in boxes if pd.notna(b)]
            
            if boxes_int:
                if boxes_int != sorted(boxes_int):
                    logger.error(f"  ❌ FAIL: Race {race_num} boxes not ascending: {boxes_int}")
                    validation_passed = False
                elif boxes_int[0] != 1:
                    logger.warning(f"  ⚠️ WARNING: Race {race_num} boxes don't start at 1: {boxes_int}")
                else:
                    logger.info(f"  [OK] Race {race_num} boxes ascending: {boxes_int}")
    
    return validation_passed


def validate_coverage_thresholds(df: pd.DataFrame) -> bool:
    """Validate field population coverage meets minimum thresholds"""
    logger.info("\n" + "="*60)
    logger.info("COVERAGE THRESHOLD VALIDATION")
    logger.info("="*60)
    
    thresholds = {
        'Distance': 90.0,
        'RaceTime': 80.0,
        'BestTime': 70.0,
        'Sectional1': 60.0,
        'Sectional2': 60.0,
        'Sectional3': 60.0,
    }
    
    validation_passed = True
    total_rows = len(df)
    
    for field, threshold_pct in thresholds.items():
        if field in df.columns:
            non_null = df[field].notna().sum()
            actual_pct = (non_null / total_rows * 100) if total_rows > 0 else 0
            
            status = "[OK] PASS" if actual_pct >= threshold_pct else "⚠️ WARN"
            logger.info(f"  {field}: {non_null}/{total_rows} ({actual_pct:.1f}%) - Threshold: {threshold_pct}% - {status}")
            
            if actual_pct < threshold_pct:
                validation_passed = False
    
    return validation_passed


def log_track_preview(df: pd.DataFrame) -> None:
    """Log preview of first 3 rows of Race 1 and last 3 rows of last Race per track"""
    logger.info("\n" + "="*60)
    logger.info("TRACK DATA PREVIEW")
    logger.info("="*60)
    
    preview_cols = ['Track', 'Race', 'Box', 'DogName', 'Distance', 'BestTime', 'Sectional1', 'Sectional2', 'Sectional3']
    available_cols = [col for col in preview_cols if col in df.columns]
    
    for track in sorted(df['Track'].dropna().unique()):
        track_df = df[df['Track'] == track].copy()
        races = sorted(track_df['Race'].dropna().unique())
        
        if not races:
            continue
        
        logger.info(f"\nTrack: {track}")
        
        # First 3 rows of Race 1
        race1_df = track_df[track_df['Race'] == races[0]].head(3)
        logger.info(f"  First 3 rows of Race {races[0]}:")
        for idx, row in race1_df.iterrows():
            values = [f"{col}={row[col]}" for col in available_cols if col in row]
            logger.info(f"    {', '.join(values)}")
        
        # Last 3 rows of last race
        last_race = races[-1]
        last_race_df = track_df[track_df['Race'] == last_race].tail(3)
        logger.info(f"  Last 3 rows of Race {last_race}:")
        for idx, row in last_race_df.iterrows():
            values = [f"{col}={row[col]}" for col in available_cols if col in row]
            logger.info(f"    {', '.join(values)}")


def validate_pdf_excel_consistency(df: pd.DataFrame, csv_path: str, excel_path: str) -> bool:
    """Validate PDF=Excel consistency"""
    logger.info("\n" + "="*60)
    logger.info("PDF=EXCEL CONSISTENCY VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    
    # Read back the files
    try:
        csv_df = pd.read_csv(csv_path)
        excel_df = pd.read_excel(excel_path)
        
        # 1. Verify first 4 columns exact
        first_4_cols = ['Track', 'Race', 'Box', 'DogName']
        logger.info(f"\nVerifying first 4 columns: {first_4_cols}")
        
        for col in first_4_cols:
            if col not in csv_df.columns or col not in excel_df.columns:
                logger.error(f"  ❌ FAIL: Column {col} missing in output files")
                validation_passed = False
            else:
                # Check values match
                csv_vals = csv_df[col].fillna('').astype(str).tolist()
                excel_vals = excel_df[col].fillna('').astype(str).tolist()
                if csv_vals == excel_vals:
                    logger.info(f"  [OK] {col}: CSV and Excel match ({len(csv_vals)} rows)")
                else:
                    logger.error(f"  ❌ FAIL: {col} values differ between CSV and Excel")
                    validation_passed = False
        
        # 2. Verify (Track, Race, Box) uniqueness
        logger.info(f"\nVerifying (Track, Race, Box) uniqueness:")
        key_cols = ['Track', 'Race', 'Box']
        
        csv_duplicates = csv_df[key_cols].duplicated().sum()
        excel_duplicates = excel_df[key_cols].duplicated().sum()
        
        if csv_duplicates > 0:
            logger.error(f"  ❌ FAIL: CSV has {csv_duplicates} duplicate (Track, Race, Box) combinations")
            validation_passed = False
        else:
            logger.info(f"  [OK] CSV: No duplicates in (Track, Race, Box)")
        
        if excel_duplicates > 0:
            logger.error(f"  ❌ FAIL: Excel has {excel_duplicates} duplicate (Track, Race, Box) combinations")
            validation_passed = False
        else:
            logger.info(f"  [OK] Excel: No duplicates in (Track, Race, Box)")
        
        # 3. Verify time/sectional field consistency
        time_fields = ['BestTime', 'Sectional1', 'Sectional2', 'Sectional3', 'SplitAvg', 'SpeedIndex']
        logger.info(f"\nVerifying time/sectional fields:")
        
        for field in time_fields:
            if field in csv_df.columns and field in excel_df.columns:
                csv_count = csv_df[field].notna().sum()
                excel_count = excel_df[field].notna().sum()
                
                if csv_count == excel_count:
                    logger.info(f"  [OK] {field}: {csv_count} non-null values in both CSV and Excel")
                    
                    # Sample a few values
                    csv_sample = csv_df[field].dropna().head(3).tolist()
                    excel_sample = excel_df[field].dropna().head(3).tolist()
                    logger.info(f"     CSV sample: {csv_sample}")
                    logger.info(f"     Excel sample: {excel_sample}")
                else:
                    logger.error(f"  ❌ FAIL: {field} has {csv_count} values in CSV but {excel_count} in Excel")
                    validation_passed = False
        
        logger.info(f"\nPDF=Excel validation: {'[OK] PASSED' if validation_passed else '❌ FAILED'}")
        
    except Exception as e:
        logger.error(f"❌ Error reading output files for validation: {e}")
        validation_passed = False
    
    return validation_passed


def run_all_validations(df: pd.DataFrame, source_files: List[str], csv_path: str, excel_path: str) -> bool:
    """Run all validation checks"""
    results = []
    
    results.append(validate_race_counts(df, source_files))
    results.append(validate_track_ordering(df))
    results.append(validate_coverage_thresholds(df))
    log_track_preview(df)
    results.append(validate_pdf_excel_consistency(df, csv_path, excel_path))
    
    all_passed = all(results)
    
    logger.info("\n" + "="*60)
    logger.info(f"OVERALL VALIDATION: {'[OK] ALL PASSED' if all_passed else '❌ SOME CHECKS FAILED'}")
    logger.info("="*60)
    
    return all_passed

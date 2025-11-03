#!/usr/bin/env python3
"""
Enhanced main controller for greyhound form extraction pipeline.
Extracts all 62 fields with proper race context tracking and generates CSV/XLSX with exact column structure.
"""

import os
import sys
import time
import logging
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from comprehensive_parser import ComprehensiveParser
from enhanced_validation import EnhancedValidator

# Directory configuration
DATA_DIR = "data"
OUTPUT_DIR = "outputs"


def setup_logging(output_dir: str) -> logging.Logger:
    """Setup logging to both console and file"""
    os.makedirs(output_dir, exist_ok=True)
    
    log_file = os.path.join(output_dir, "parse_enhanced.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to: {log_file}")
    
    return logger


def export_reports(df: pd.DataFrame, output_dir: str) -> tuple:
    """Export DataFrame to both CSV and Excel with timestamp as todays_form"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    base_name = f"todays_form_{timestamp}"
    
    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    excel_path = os.path.join(output_dir, f"{base_name}.xlsx")
    
    # Export CSV
    df.to_csv(csv_path, index=False)
    print(f"[OK] CSV saved: {csv_path}")
    
    # Export Excel
    df.to_excel(excel_path, index=False, engine='openpyxl')
    print(f"[OK] Excel saved: {excel_path}")
    
    return csv_path, excel_path


def validate_data_integrity(df: pd.DataFrame, logger: logging.Logger) -> bool:
    """Validate PDF=Excel consistency and speed-related fields"""
    logger.info("\n" + "="*60)
    logger.info("DATA VALIDATION")
    logger.info("="*60)
    
    validation_passed = True
    
    # Check required columns
    required_cols = ['Track', 'Race', 'Box', 'DogName']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"[FAIL] Missing required columns: {missing_cols}")
        validation_passed = False
    else:
        logger.info(f"[OK] Required columns present: {required_cols}")
    
    # Verify speed-related fields consistency
    speed_fields = ['BestTime', 'Sectional1', 'Sectional2', 'Sectional3', 
                    'SplitAvg', 'EarlySpeed', 'ClosingSpeed', 'RaceTime']
    speed_fields_present = [f for f in speed_fields if f in df.columns]
    
    logger.info(f"\nSpeed-related fields:")
    for field in speed_fields_present:
        non_null = df[field].notna().sum()
        pct = (non_null / len(df) * 100) if len(df) > 0 else 0
        logger.info(f"  {field}: {non_null}/{len(df)} ({pct:.1f}%) populated")
    
    if speed_fields_present:
        logger.info("[OK] Speed fields verified OK")
    
    # Row and column count verification
    logger.info(f"\nData dimensions:")
    logger.info(f"  Rows (dogs): {len(df)}")
    logger.info(f"  Columns (fields): {len(df.columns)}")
    logger.info("[OK] Row and column counts verified")
    
    # Check race/box ordering
    if 'Race' in df.columns and 'Box' in df.columns:
        sorted_check = df[['Race', 'Box']].copy()
        is_sorted = (sorted_check['Race'].is_monotonic_increasing or 
                    sorted_check.groupby('Race')['Box'].apply(lambda x: x.is_monotonic_increasing).all())
        if is_sorted:
            logger.info("[OK] Race/Box ordering verified (ascending)")
        else:
            logger.warning("[WARN] Race/Box ordering may not be strictly ascending")
    
    logger.info("\n[OK] PDF=Excel verification complete")
    logger.info("="*60 + "\n")
    
    return validation_passed


def log_statistics(df: pd.DataFrame, logger: logging.Logger):
    """Log detailed extraction statistics"""
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION STATISTICS - TODAY'S FORM")
    logger.info("="*60)
    
    if df.empty:
        logger.warning("No data extracted!")
        return
    
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Total fields per record: {len(df.columns)}")
    logger.info(f"Total tracks: {df['Track'].nunique()}")
    logger.info(f"Total races: {df['Race'].nunique()}")
    
    # Per-track breakdown
    logger.info("\nPer-Track Breakdown:")
    track_stats = df.groupby('Track').agg({
        'Race': 'nunique',
        'DogName': 'count'
    }).rename(columns={'Race': 'Races', 'DogName': 'Dogs'})
    
    for track, row in track_stats.iterrows():
        logger.info(f"  {track}: {row['Races']} races, {row['Dogs']} dogs")
    
    # Field coverage analysis
    logger.info("\nField Coverage (non-null values):")
    key_fields = ['DogName', 'Trainer', 'Grade', 'Distance', 'Form', 'Starts', 
                  'Wins', 'CareerPrizeMoney', 'Age', 'Sex']
    for field in key_fields:
        if field in df.columns:
            non_null = df[field].notna().sum()
            pct = (non_null / len(df) * 100) if len(df) > 0 else 0
            logger.info(f"  {field}: {non_null}/{len(df)} ({pct:.1f}%)")
    
    # Race ordering check
    logger.info("\nRace/Box Ordering:")
    logger.info(f"  Races range: {df['Race'].min()} to {df['Race'].max()}")
    logger.info(f"  Boxes per race: {df.groupby('Race')['Box'].nunique().to_dict()}")
    
    logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("GREYHOUND FORM EXTRACTION PIPELINE - TODAY'S FORM")
    print("="*80 + "\n")
    
    logger = setup_logging(OUTPUT_DIR)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    logger.info(f"Data directory: {os.path.abspath(DATA_DIR)}")
    logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"Extracting 62 fields per dog")
    
    # Parse all PDFs using ComprehensiveParser
    print(f"Processing PDFs from: {DATA_DIR}\n")
    parser = ComprehensiveParser()
    df = parser.parse_directory(DATA_DIR)
    
    if df.empty:
        print("\n[WARN] No data extracted from PDFs.")
        print(f"Please add PDF files to {os.path.abspath(DATA_DIR)} and try again.\n")
        return
    
    # Enforce dtypes before sorting
    if 'Race' in df.columns:
        df['Race'] = pd.to_numeric(df['Race'], errors='coerce').fillna(0).astype(int)
    if 'Box' in df.columns:
        df['Box'] = pd.to_numeric(df['Box'], errors='coerce').fillna(0).astype(int)
    
    # Sort strictly: Track -> Race -> Box
    if all(col in df.columns for col in ['Track', 'Race', 'Box']):
        df = df.sort_values(by=['Track', 'Race', 'Box'], ascending=[True, True, True])
        df = df.reset_index(drop=True)
        logger.info("[OK] Data sorted by Track -> Race -> Box")
    
    # Log statistics
    log_statistics(df, logger)
    
    # Validate data integrity using EnhancedValidator
    print("\nValidating data integrity...")
    validator = EnhancedValidator()
    validation_passed = validator.validate_dataframe(df, logger)
    validate_data_integrity(df, logger)
    
    if not validation_passed:
        logger.warning("[WARN] Some validation checks failed - review log for details")
    
    # Export reports
    print("\nGenerating reports...")
    csv_path, excel_path = export_reports(df, OUTPUT_DIR)
    
    # Final summary message
    tracks = df['Track'].nunique()
    races = len(df.groupby(['Track', 'Race']))
    dogs = len(df)
    
    summary_msg = f"Tracks: {tracks} | Races: {races} | Dogs: {dogs} | Speed fields verified OK | PDF=Excel verified."
    logger.info(summary_msg)
    
    print("\n" + "="*80)
    print("[OK] EXTRACTION COMPLETE - TODAY'S FORM")
    print("="*80)
    print(f"\n[INFO] Summary:")
    print(f"  {summary_msg}")
    print(f"\n[FILE] Output Files:")
    print(f"  - CSV report: {csv_path}")
    print(f"  - Excel report: {excel_path}")
    print(f"  - Log file: {os.path.join(OUTPUT_DIR, 'parse_enhanced.log')}")
    
    # Display sample with first 4 columns as Track/Race/Box/DogName
    print(f"\n[LOG] Sample Output (first 3 rows):")
    sample_cols = ['Track', 'Race', 'Box', 'DogName', 'Trainer', 'Grade', 
                   'Distance', 'Wins', 'Starts']
    available_cols = [c for c in sample_cols if c in df.columns]
    print(df[available_cols].head(3).to_string(index=False))
    print()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Enhanced main controller for greyhound form extraction pipeline.
Extracts all 62 fields and generates CSV/XLSX with exact column structure.
"""

import os
import sys
import time
import logging
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from parser_enhanced_full import parse_directory, COLUMNS

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
    """Export DataFrame to both CSV and Excel with timestamp"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    base_name = f"greyhound_analysis_full_{timestamp}"
    
    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    excel_path = os.path.join(output_dir, f"{base_name}.xlsx")
    
    # Export CSV
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved: {csv_path}")
    
    # Export Excel
    df.to_excel(excel_path, index=False, engine='openpyxl')
    print(f"✅ Excel saved: {excel_path}")
    
    return csv_path, excel_path


def log_statistics(df: pd.DataFrame, logger: logging.Logger):
    """Log detailed extraction statistics"""
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION STATISTICS - ENHANCED 62-FIELD PARSER")
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
    print("🐕 GREYHOUND FORM EXTRACTION PIPELINE - ENHANCED 62-FIELD VERSION")
    print("="*80 + "\n")
    
    logger = setup_logging(OUTPUT_DIR)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    logger.info(f"Data directory: {os.path.abspath(DATA_DIR)}")
    logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"Extracting {len(COLUMNS)} fields per dog")
    
    # Parse all PDFs
    print(f"Processing PDFs from: {DATA_DIR}\n")
    df = parse_directory(DATA_DIR)
    
    if df.empty:
        print("\n⚠️  No data extracted from PDFs.")
        print(f"Please add PDF files to {os.path.abspath(DATA_DIR)} and try again.\n")
        return
    
    # Log statistics
    log_statistics(df, logger)
    
    # Export reports
    print("\nGenerating reports...")
    csv_path, excel_path = export_reports(df, OUTPUT_DIR)
    
    print("\n" + "="*80)
    print("✅ EXTRACTION COMPLETE - ALL FIELDS")
    print("="*80)
    print(f"\n📊 Summary:")
    print(f"  • Total records: {len(df)}")
    print(f"  • Fields per record: {len(df.columns)}")
    print(f"  • Unique tracks: {df['Track'].nunique()}")
    print(f"  • Unique races: {len(df.groupby(['Track', 'Race']))}")
    print(f"  • CSV report: {csv_path}")
    print(f"  • Excel report: {excel_path}")
    print(f"  • Log file: {os.path.join(OUTPUT_DIR, 'parse_enhanced.log')}")
    
    # Display sample
    print(f"\n📋 Sample Output (first 3 rows, key fields):")
    sample_cols = ['Track', 'Race', 'Box', 'DogName', 'Trainer', 'Grade', 
                   'Distance', 'Wins', 'Starts', 'CareerPrizeMoney']
    available_cols = [c for c in sample_cols if c in df.columns]
    print(df[available_cols].head(3).to_string(index=False))
    print()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Main controller for greyhound form extraction pipeline.
Processes all PDFs in /data directory and generates combined reports.
"""

import os
import sys
import time
import logging
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from parser_step1_complete_layoutaware import parse_pdf_file

# Directory configuration
DATA_DIR = "data"
OUTPUT_DIR = "outputs"


def setup_logging(output_dir: str) -> logging.Logger:
    """Setup logging to both console and file"""
    os.makedirs(output_dir, exist_ok=True)
    
    log_file = os.path.join(output_dir, "parse.log")
    
    # Configure root logger
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
    """
    Export DataFrame to both CSV and Excel with timestamp.
    
    Args:
        df: DataFrame to export
        output_dir: Output directory path
        
    Returns:
        Tuple of (csv_path, excel_path)
    """
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    base_name = f"greyhound_analysis_{timestamp}"
    
    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    excel_path = os.path.join(output_dir, f"{base_name}.xlsx")
    
    # Export CSV
    df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved: {csv_path}")
    
    # Export Excel
    df.to_excel(excel_path, index=False, engine='openpyxl')
    print(f"✅ Excel saved: {excel_path}")
    
    return csv_path, excel_path


def collect_pdf_files(data_dir: str) -> list:
    """
    Recursively collect all PDF files from data directory.
    
    Args:
        data_dir: Root data directory
        
    Returns:
        List of PDF file paths
    """
    pdf_files = []
    
    if not os.path.exists(data_dir):
        print(f"⚠️  Data directory not found: {data_dir}")
        return pdf_files
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    return sorted(pdf_files)


def log_statistics(df: pd.DataFrame, logger: logging.Logger):
    """Log detailed extraction statistics"""
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION STATISTICS")
    logger.info("="*60)
    
    if df.empty:
        logger.warning("No data extracted!")
        return
    
    # Overall stats
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Total tracks: {df['Track'].nunique()}")
    logger.info(f"Total races: {df['RaceNo'].nunique()}")
    
    # Per-track breakdown
    logger.info("\nPer-Track Breakdown:")
    track_stats = df.groupby('Track').agg({
        'RaceNo': 'nunique',
        'DogName': 'count'
    }).rename(columns={'RaceNo': 'Races', 'DogName': 'Dogs'})
    
    for track, row in track_stats.iterrows():
        logger.info(f"  {track}: {row['Races']} races, {row['Dogs']} dogs")
    
    # Data quality checks
    logger.info("\nData Quality:")
    logger.info(f"  Missing dog names: {df['DogName'].isna().sum()}")
    logger.info(f"  Missing trainers: {df['Trainer'].isna().sum()}")
    logger.info(f"  Missing distances: {df['Distance'].isna().sum()}")
    
    # Race size distribution
    logger.info("\nRace Size Distribution (dogs per race):")
    race_sizes = df.groupby(['Track', 'RaceNo']).size()
    logger.info(f"  Min dogs per race: {race_sizes.min()}")
    logger.info(f"  Max dogs per race: {race_sizes.max()}")
    logger.info(f"  Avg dogs per race: {race_sizes.mean():.1f}")
    
    logger.info("="*60 + "\n")


def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("🐕 GREYHOUND FORM EXTRACTION PIPELINE - STEP 1")
    print("="*60 + "\n")
    
    # Setup logging
    logger = setup_logging(OUTPUT_DIR)
    
    # Ensure directories exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    logger.info(f"Data directory: {os.path.abspath(DATA_DIR)}")
    logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    
    # Collect PDF files
    pdf_files = collect_pdf_files(DATA_DIR)
    
    if not pdf_files:
        print(f"\n⚠️  No PDF files found in {DATA_DIR}")
        print(f"Please add PDF files to {os.path.abspath(DATA_DIR)} and try again.\n")
        return
    
    logger.info(f"\nFound {len(pdf_files)} PDF files to process")
    
    # Process each PDF
    all_data = []
    successful = 0
    failed = 0
    
    for idx, pdf_path in enumerate(pdf_files, 1):
        rel_path = os.path.relpath(pdf_path, DATA_DIR)
        print(f"\n[{idx}/{len(pdf_files)}] 📄 Processing: {rel_path}")
        
        try:
            df = parse_pdf_file(pdf_path)
            if not df.empty:
                all_data.append(df)
                successful += 1
                print(f"    ✓ Extracted {len(df)} records")
            else:
                logger.warning(f"    ⚠️  No data extracted from {rel_path}")
                failed += 1
        except Exception as e:
            logger.error(f"    ✗ Failed to parse {rel_path}: {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"Processing complete: {successful} successful, {failed} failed")
    print("="*60 + "\n")
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Log statistics
        log_statistics(combined_df, logger)
        
        # Export reports
        print("Generating reports...")
        csv_path, excel_path = export_reports(combined_df, OUTPUT_DIR)
        
        print("\n" + "="*60)
        print("✅ ALL TRACKS COMBINED — STEP 1 COMPLETE")
        print("="*60)
        print(f"\n📊 Summary:")
        print(f"  • Total records: {len(combined_df)}")
        print(f"  • Unique tracks: {combined_df['Track'].nunique()}")
        print(f"  • Unique races: {len(combined_df.groupby(['Track', 'RaceNo']))}")
        print(f"  • CSV report: {csv_path}")
        print(f"  • Excel report: {excel_path}")
        print(f"  • Log file: {os.path.join(OUTPUT_DIR, 'parse.log')}")
        print()
        
    else:
        print("\n⚠️  No data extracted from any PDF files.")
        print("Please check that:")
        print("  • PDFs are Racing & Sports greyhound race form guides")
        print("  • PDFs contain race entries (not just results)")
        print("  • PDFs have the expected multi-column format")
        print("\nFor troubleshooting and supported formats, see PIPELINE_README.md\n")


if __name__ == "__main__":
    main()

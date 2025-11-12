#!/usr/bin/env python3
"""
mastercontrol.py
================
Main automation script for the Greyhound PDF ingestion system.

This script orchestrates the complete pipeline:
1. Fetch daily Australian greyhound race form PDFs
2. Parse PDFs to extract structured race data
3. Generate reports and probabilities
4. Save outputs to data/output/

Usage:
    python src/mastercontrol.py                    # Run full pipeline for today
    python src/mastercontrol.py --date 2025-09-01  # Run for specific date
    python src/mastercontrol.py --help             # Show help
"""

from __future__ import annotations
import argparse
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from fetch_forms import fetch_all
    from parse_pdf import parse_folder
    from export_to_excel import export_to_excel
    from extractor_text import extract_summary_data
    from extractor_table import extract_history_data
except ImportError as e:
    print(f"Error: Could not import required modules: {e}")
    print("Make sure you are running from the repository root.")
    sys.exit(1)

import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_directories() -> tuple[Path, Path, Path]:
    """Create and return paths for input, output, and forms directories."""
    root = Path(__file__).resolve().parent.parent
    input_dir = root / "data" / "input"
    output_dir = root / "data" / "output"
    forms_dir = root / "forms"
    
    for d in [input_dir, output_dir, forms_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    return input_dir, output_dir, forms_dir


def build_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate uniform win probabilities for each race.
    
    Args:
        df: DataFrame with columns [track, date, race, box, runner]
    
    Returns:
        DataFrame with added prob_win column
    """
    if df.empty:
        return pd.DataFrame(columns=["track", "date", "race", "box", "runner", "prob_win"])
    
    rows = []
    for (track, date, race), group in df.groupby(["track", "date", "race"], sort=True):
        n_runners = max(len(group), 1)
        prob = round(1.0 / n_runners, 6)
        
        for _, row in group.iterrows():
            rows.append({
                "track": track,
                "date": date,
                "race": int(race),
                "box": int(row["box"]),
                "runner": row["runner"],
                "prob_win": prob
            })
    
    return (pd.DataFrame(rows)
            .sort_values(["track", "race", "box"])
            .reset_index(drop=True))


def generate_summary(prob_df: pd.DataFrame, output_dir: Path) -> None:
    """
    Generate a markdown summary report with top picks per race.
    
    Args:
        prob_df: DataFrame with probability data
        output_dir: Directory to save summary
    """
    summary_path = output_dir / "summary.md"
    
    if prob_df.empty:
        summary_path.write_text("## Summary — No data available\n", encoding="utf-8")
        print(f"[mastercontrol] Wrote empty summary to {summary_path}")
        return
    
    # Get the latest date in the data
    latest_date = sorted(prob_df["date"].unique())[-1] if len(prob_df["date"].unique()) > 0 else "Unknown"
    
    lines = [
        f"# Greyhound Race Summary — {latest_date}",
        "",
        "## Top Picks (Uniform Probability Baseline)",
        ""
    ]
    
    # Group by track and race
    for (track, date, race), group in prob_df.groupby(["track", "date", "race"], sort=True):
        # Get top pick (highest probability, lowest box on tie)
        top_pick = group.sort_values(["prob_win", "box"], ascending=[False, True]).iloc[0]
        
        lines.append(
            f"- **{track}** Race {int(race)} → "
            f"Box {int(top_pick['box'])} — {top_pick['runner']} "
            f"(p={top_pick['prob_win']:.3f})"
        )
    
    lines.append("")
    lines.append(f"## Statistics")
    lines.append(f"- Total tracks: {prob_df['track'].nunique()}")
    lines.append(f"- Total races: {len(prob_df.groupby(['track', 'race']))}")
    lines.append(f"- Total runners: {len(prob_df)}")
    lines.append("")
    
    summary_text = "\n".join(lines)
    summary_path.write_text(summary_text, encoding="utf-8")
    print(f"[mastercontrol] Wrote summary to {summary_path}")


def main() -> int:
    """Main entry point for the automation script."""
    parser = argparse.ArgumentParser(
        description="Mastercontrol Greyhound - Automated PDF Ingestion System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    Run full pipeline for today
  %(prog)s --date 2025-09-01  Run for specific date
  %(prog)s --skip-fetch       Only parse existing PDFs
        """
    )
    parser.add_argument(
        "--date",
        help="Specific date to process (YYYY-MM-DD). Default: today (AU time)"
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip PDF fetching, only parse existing PDFs"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Setup
    input_dir, output_dir, forms_dir = setup_directories()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    print("=" * 70)
    print("Mastercontrol Greyhound - Automated PDF Ingestion System")
    print("=" * 70)
    
    # Step 1: Fetch PDFs (unless skipped)
    if not args.skip_fetch:
        print("\n[1/4] Fetching race form PDFs...")
        try:
            # Set environment variable if date override provided
            if args.date:
                os.environ["FORCE_DATE"] = args.date
                print(f"      Using date override: {args.date}")
            
            saved_count = fetch_all(forms_dir)
            print(f"      ✓ Fetched {saved_count} valid PDF(s)")
            
            if saved_count == 0:
                print("\n⚠ Warning: No PDFs were downloaded.")
                print("  This might be normal if there are no meetings today.")
                print("  The pipeline will continue with existing PDFs if any.")
        except Exception as e:
            print(f"      ✗ Error fetching PDFs: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            print("\n  Continuing with existing PDFs...")
    else:
        print("\n[1/4] Skipping PDF fetch (--skip-fetch enabled)")
    
    # Step 2: Parse PDFs
    print("\n[2/4] Parsing PDFs...")
    try:
        parsed_df = parse_folder(str(forms_dir))
        print(f"      ✓ Parsed {len(parsed_df)} row(s) from PDFs")
        
        if parsed_df.empty:
            print("\n⚠ Warning: No data extracted from PDFs.")
            print("  Check that PDFs exist in the forms/ directory.")
            # Write empty outputs
            output_csv = output_dir / f"parsed_{timestamp}.csv"
            parsed_df.to_csv(output_csv, index=False)
            generate_summary(pd.DataFrame(), output_dir)
            print("\n" + "=" * 70)
            print("Pipeline completed with no data")
            print("=" * 70)
            return 0
        
        # Save parsed data
        output_csv = output_dir / f"parsed_{timestamp}.csv"
        parsed_df.to_csv(output_csv, index=False)
        print(f"      ✓ Saved to {output_csv}")
        
    except Exception as e:
        print(f"      ✗ Error parsing PDFs: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    # Step 3: Generate reports
    print("\n[3/4] Generating reports...")
    try:
        # Calculate probabilities
        prob_df = build_probabilities(parsed_df)
        prob_csv = output_dir / "probabilities.csv"
        prob_df.to_csv(prob_csv, index=False)
        print(f"      ✓ Saved probabilities to {prob_csv}")
        
        # Generate summary
        generate_summary(prob_df, output_dir)
        
    except Exception as e:
        print(f"      ✗ Error generating reports: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    # Step 4: Enhanced PDF Extraction and Excel Export
    print("\n[4/4] Enhanced extraction and Excel export...")
    try:
        # Get list of PDF files
        pdf_files = list(forms_dir.glob("*.pdf"))
        if not pdf_files:
            print("      ⚠ No PDF files found in forms directory")
            logger.warning("No PDFs to process")
        else:
            logger.info(f"Found {len(pdf_files)} PDF files to process")
            print(f"      Processing {len(pdf_files)} PDF file(s)...")
            
            all_summary_dfs = []
            all_history_dfs = []
            
            # Process each PDF with enhanced extractors
            for pdf_file in pdf_files:
                logger.info(f"Processing {pdf_file.name}")
                print(f"      - {pdf_file.name}")
                
                try:
                    # Extract summary data (Groups A & B)
                    summary_df = extract_summary_data(pdf_file)
                    all_summary_dfs.append(summary_df)
                    logger.info(f"  Extracted {len(summary_df)} dogs from {pdf_file.name}")
                    
                    # Extract history data (Group C)
                    history_df = extract_history_data(pdf_file)
                    all_history_dfs.append(history_df)
                    logger.info(f"  Extracted {len(history_df)} history records from {pdf_file.name}")
                    
                except Exception as e:
                    logger.error(f"  Error processing {pdf_file.name}: {e}")
                    if args.verbose:
                        import traceback
                        traceback.print_exc()
            
            # Combine all DataFrames
            if all_summary_dfs:
                combined_summary = pd.concat(all_summary_dfs, ignore_index=True)
                logger.info(f"Total dogs extracted: {len(combined_summary)}")
                print(f"      ✓ Extracted {len(combined_summary)} total dogs")
            else:
                combined_summary = pd.DataFrame()
                logger.warning("No summary data extracted")
            
            if all_history_dfs:
                combined_history = pd.concat(all_history_dfs, ignore_index=True)
                logger.info(f"Total history records: {len(combined_history)}")
                print(f"      ✓ Extracted {len(combined_history)} total history records")
            else:
                combined_history = pd.DataFrame()
                logger.warning("No history data extracted")
            
            # Generate Excel filename with timestamp
            excel_path = output_dir / f"greyhound_results_{timestamp}.xlsx"
            
            # Export to Excel
            if not combined_summary.empty or not combined_history.empty:
                export_to_excel(combined_summary, combined_history, excel_path)
                print(f"      ✓ Excel export completed")
                logger.info(f"Excel file created: {excel_path}")
                
                # Log statistics
                print("\n      Excel Statistics:")
                print(f"        - Dog Summary rows: {len(combined_summary)}")
                print(f"        - Race History rows: {len(combined_history)}")
                if not combined_summary.empty:
                    print(f"        - Unique dogs: {combined_summary['Dog_Name'].nunique()}")
                if not combined_history.empty:
                    print(f"        - Dogs with history: {combined_history['Dog_Name'].nunique()}")
            else:
                logger.warning("No data to export to Excel")
                print("      ⚠ No data available for Excel export")
        
    except Exception as e:
        print(f"      ✗ Error in enhanced extraction: {e}")
        logger.error(f"Enhanced extraction error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        # Don't fail the entire pipeline if Excel export fails
        print("      Continuing despite Excel export error...")
    
    # Summary
    print("\n" + "=" * 70)
    print("Pipeline completed successfully!")
    print("=" * 70)
    print(f"\nOutputs:")
    print(f"  - Parsed data:     {output_csv}")
    print(f"  - Probabilities:   {prob_csv}")
    print(f"  - Summary report:  {output_dir / 'summary.md'}")
    print(f"  - Excel export:    {output_dir / f'greyhound_results_{timestamp}.xlsx'}")
    print(f"\nStatistics:")
    print(f"  - Tracks processed: {parsed_df['track'].nunique()}")
    print(f"  - Total races:      {len(parsed_df.groupby(['track', 'race']))}")
    print(f"  - Total runners:    {len(parsed_df)}")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

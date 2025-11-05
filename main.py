#!/usr/bin/env python3
"""
main.py - Main entry point for Greyhound race form processing

This script orchestrates the entire workflow:
1. Fetch race form PDFs from Racing & Sports
2. Parse PDF files to extract race data
3. Generate probability predictions
4. Create reports and summaries

Usage:
    python main.py [--date YYYY-MM-DD] [--forms-dir DIR] [--output-dir DIR]
"""

import os
import sys
import argparse
import datetime as dt
import pandas as pd
from pathlib import Path
from dateutil import tz

# Import parser module
from parser.pdf_parser import parse_folder, parse_pdf

# Try to import fetch functionality
try:
    from src.fetch_forms import fetch_all as src_fetch_all, sydney_today
except ImportError:
    try:
        from fetch_forms import fetch_all as src_fetch_all
        from dateutil.tz import gettz
        
        def sydney_today():
            """Get today's date in Sydney timezone."""
            syd_tz = gettz("Australia/Sydney")
            now_syd = dt.datetime.now(syd_tz)
            return now_syd.strftime("%Y-%m-%d")
    except ImportError:
        def src_fetch_all(out_dir):
            print("[main] Warning: fetch_forms module not available")
            return 0
        
        def sydney_today():
            return dt.datetime.now().strftime("%Y-%m-%d")


def ensure_dirs(*paths):
    """Create directories if they don't exist."""
    for p in paths:
        os.makedirs(p, exist_ok=True)


def uniform_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate uniform baseline probabilities for runners.
    Assumes 8 boxes per race with equal probability.
    """
    if df.empty:
        return df.assign(prob_win=pd.Series([], dtype=float))
    # Assume 8 boxes; uniform baseline
    return df.assign(prob_win=1.0/8.0)


def calculate_advanced_probabilities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate more advanced probabilities based on available data.
    For now, uses uniform distribution as baseline.
    Can be extended with ML models or historical data.
    """
    if df.empty:
        return df
    
    # Start with uniform probabilities
    df = uniform_probabilities(df)
    
    # Placeholder for future enhancements:
    # - Historical performance analysis
    # - Track conditions
    # - Trainer/jockey statistics
    # - Form analysis
    
    return df


def generate_summary_report(probs: pd.DataFrame, date_str: str) -> str:
    """Generate a markdown summary of top picks."""
    if probs.empty:
        return f"# Summary — {date_str}\n\nNo data available.\n"
    
    lines = [f"# Summary — {date_str}", ""]
    lines.append("## Top Picks by Race")
    lines.append("")
    
    # Group by track and race, show top pick for each
    for (track, race), group in probs.groupby(["track", "race"]):
        pick = group.sort_values(["prob_win", "box"], ascending=[False, True]).iloc[0]
        lines.append(
            f"- **{track} R{int(race)}** → Box {int(pick['box'])} — {pick['runner']} "
            f"(p={pick['prob_win']:.3f})"
        )
    
    lines.append("")
    lines.append("## Statistics")
    lines.append(f"- Total tracks: {probs['track'].nunique()}")
    lines.append(f"- Total races: {len(probs.groupby(['track', 'race']))}")
    lines.append(f"- Total runners: {len(probs)}")
    
    return "\n".join(lines) + "\n"


def write_reports(df: pd.DataFrame, out_root: str, date_str: str):
    """Write probability reports and summaries to output directories."""
    today_dir = os.path.join(out_root, date_str)
    latest_dir = os.path.join(out_root, "latest")
    ensure_dirs(today_dir, latest_dir)

    if df.empty:
        summary = f"# Summary — {date_str}\n\nNo data available.\n"
        with open(os.path.join(latest_dir, "summary.md"), "w") as f:
            f.write(summary)
        with open(os.path.join(today_dir, "summary.md"), "w") as f:
            f.write(summary)
        return

    # Calculate probabilities
    probs = calculate_advanced_probabilities(df)
    
    # Save probability CSV files
    probs_path_today = os.path.join(today_dir, "probabilities.csv")
    probs_path_latest = os.path.join(latest_dir, "probabilities.csv")
    probs.to_csv(probs_path_today, index=False)
    probs.to_csv(probs_path_latest, index=False)
    print(f"[main] Saved probabilities to {probs_path_today}")

    # Generate and save summary
    summary = generate_summary_report(probs, date_str)
    with open(os.path.join(today_dir, "summary.md"), "w") as f:
        f.write(summary)
    with open(os.path.join(latest_dir, "summary.md"), "w") as f:
        f.write(summary)
    print(f"[main] Saved summary to {today_dir}/summary.md")


def fetch_forms(forms_dir: str, date_str: str) -> int:
    """Fetch race form PDFs for the specified date."""
    print(f"[main] Fetching forms for {date_str}...")
    forms_path = Path(forms_dir)
    forms_path.mkdir(parents=True, exist_ok=True)
    
    try:
        count = src_fetch_all(forms_path)
        print(f"[main] Fetched {count} form PDFs")
        return count
    except Exception as e:
        print(f"[main] Error fetching forms: {e}")
        return 0


def parse_forms(forms_dir: str, data_dir: str, date_str: str) -> pd.DataFrame:
    """Parse all PDF forms in the forms directory."""
    print(f"[main] Parsing forms from {forms_dir}...")
    
    try:
        parsed = parse_folder(forms_dir)
        print(f"[main] Parsed {len(parsed)} runner entries")
        
        if not parsed.empty:
            # Save parsed data
            ensure_dirs(data_dir)
            parsed_path = os.path.join(data_dir, f"parsed_{date_str}.csv")
            parsed.to_csv(parsed_path, index=False)
            print(f"[main] Saved parsed data to {parsed_path}")
        
        return parsed
    except Exception as e:
        print(f"[main] Error parsing forms: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Greyhound race form processing and prediction system"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to today in Sydney timezone."
    )
    parser.add_argument(
        "--forms-dir",
        default="forms",
        help="Directory for form PDFs (default: forms)"
    )
    parser.add_argument(
        "--data-dir",
        default="data/rns",
        help="Directory for parsed data (default: data/rns)"
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory for reports and outputs (default: outputs)"
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip fetching forms (use existing PDFs)"
    )
    parser.add_argument(
        "--skip-parse",
        action="store_true",
        help="Skip parsing (use existing parsed data)"
    )
    
    args = parser.parse_args()
    
    # Determine date to process
    date_str = args.date or sydney_today()
    print(f"[main] Processing date: {date_str}")
    
    # Ensure directories exist
    ensure_dirs(args.forms_dir, args.data_dir, args.output_dir)
    
    # Step 1: Fetch forms (unless skipped)
    if not args.skip_fetch:
        fetched_count = fetch_forms(args.forms_dir, date_str)
        if fetched_count == 0:
            print("[main] Warning: No forms fetched")
    else:
        print("[main] Skipping fetch step")
    
    # Step 2: Parse forms (unless skipped)
    if not args.skip_parse:
        parsed_df = parse_forms(args.forms_dir, args.data_dir, date_str)
    else:
        print("[main] Skipping parse step, loading existing data...")
        parsed_path = os.path.join(args.data_dir, f"parsed_{date_str}.csv")
        if os.path.exists(parsed_path):
            parsed_df = pd.read_csv(parsed_path)
            print(f"[main] Loaded {len(parsed_df)} entries from {parsed_path}")
        else:
            print(f"[main] Error: No parsed data found at {parsed_path}")
            parsed_df = pd.DataFrame()
    
    # Step 3: Generate reports
    print("[main] Generating reports...")
    write_reports(parsed_df, args.output_dir, date_str)
    
    # Summary
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Date: {date_str}")
    print(f"Forms directory: {args.forms_dir}")
    print(f"Data directory: {args.data_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Entries processed: {len(parsed_df)}")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

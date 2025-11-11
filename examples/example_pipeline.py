#!/usr/bin/env python3
"""
example_pipeline.py
-------------------
Demonstration of the complete pipeline:
1. Parse Section 2 data
2. Build advanced features (Speed_kmh)
3. Validate and export

This example shows how to use the new modules together.
"""

import pandas as pd
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from parser import parse_race_card
from features_advanced import process_race_features
from exporter import validate_speed_uniqueness, export_to_csv


def create_sample_data():
    """
    Create sample race data to demonstrate the pipeline.
    
    This simulates what you would get from parsing real form PDFs.
    """
    # Race 1 at SALE - 3 dogs with different Section 2 histories
    race_1 = [
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 1,
            'Runner': 'FAST PUP',
            'section2': '400m 23.50, 520m:29.80, 450m–25.20s'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 2,
            'Runner': 'MEDIUM PUP',
            'section2': '400m 24.10, 520m:30.50'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 3,
            'Runner': 'SLOW PUP',
            'section2': '400m 25.00, 450m—26.50s'
        }
    ]
    
    # Race 2 at RICH - 3 dogs
    race_2 = [
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 1,
            'Runner': 'SPEEDY DOG',
            'section2': '520m:29.50, 450m 24.80'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 2,
            'Runner': 'AVERAGE DOG',
            'section2': '400m 23.80, 520m:31.00'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 3,
            'Runner': 'STEADY DOG',
            'section2': '450m 25.50, 400m 24.20'
        }
    ]
    
    return race_1 + race_2


def main():
    """Run the complete pipeline demonstration."""
    
    print("=" * 70)
    print("GREYHOUND SPEED CALCULATION PIPELINE DEMONSTRATION")
    print("=" * 70)
    print()
    
    # Step 1: Create sample data
    print("Step 1: Creating sample race data...")
    raw_data = create_sample_data()
    print(f"  Created {len(raw_data)} dog entries across 2 races")
    print()
    
    # Step 2: Parse Section 2 data
    print("Step 2: Parsing Section 2 (recent runs) data...")
    parsed_data = parse_race_card(raw_data)
    print(f"  Parsed Section 2 for {len(parsed_data)} dogs")
    
    # Show sample parsed data
    print("\n  Example for FAST PUP:")
    fast_pup = parsed_data[0]
    print(f"    S2_AllSpeeds: {[round(s, 2) for s in fast_pup['S2_AllSpeeds']]}")
    print(f"    S2_1_Distance: {fast_pup['S2_1_Distance']}m")
    print(f"    S2_1_RaceTime: {fast_pup['S2_1_RaceTime']}s")
    print(f"    S2_1_Speed: {fast_pup['S2_1_Speed']:.2f} km/h")
    print()
    
    # Step 3: Build advanced features
    print("Step 3: Building advanced features (Speed_kmh)...")
    featured_data = process_race_features(parsed_data)
    print(f"  Added Speed_kmh for {len(featured_data)} dogs")
    print()
    
    # Step 4: Convert to DataFrame
    print("Step 4: Converting to DataFrame...")
    df = pd.DataFrame(featured_data)
    
    # Keep only relevant columns for demonstration
    display_cols = ['Track', 'RaceNumber', 'Box', 'Runner', 'Speed_kmh', 
                    'S2_1_Distance', 'S2_1_RaceTime']
    df_display = df[display_cols].copy()
    
    print("\n" + "=" * 70)
    print("RESULTS - Speed_kmh per Dog")
    print("=" * 70)
    print(df_display.to_string(index=False))
    print()
    
    # Step 5: Validate
    print("=" * 70)
    print("VALIDATION - Check for Duplicate Speeds per Race")
    print("=" * 70)
    stats = validate_speed_uniqueness(df, verbose=True)
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total races: {stats['total_races']}")
    print(f"Races with unique speeds (GOOD): {stats['races_with_unique_speeds']}")
    print(f"Races with duplicate speeds (BAD): {stats['races_with_duplicates']}")
    print()
    
    if stats['races_with_duplicates'] == 0:
        print("✓ SUCCESS! All races have unique per-dog speeds.")
    else:
        print("✗ FAILURE! Some races still have duplicate speeds.")
    
    print("=" * 70)
    
    # Optionally export
    output_path = '/tmp/greyhound_example_output.csv'
    print(f"\nExporting to: {output_path}")
    export_to_csv(df, output_path, validate=False)  # Already validated above
    print()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

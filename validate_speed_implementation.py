#!/usr/bin/env python3
"""
validate_speed_implementation.py
---------------------------------
Validation script to verify per-dog Speed_kmh implementation.

Demonstrates:
1. Section 2 parsing from simulated form data
2. Per-dog Speed_kmh calculation
3. Validation of uniqueness per race
4. Coverage statistics
"""

import pandas as pd
import sys
import os
from typing import Dict, List

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from parser import parse_race_card, _extract_recent_runs, _normalize_section2
from features_advanced import process_race_features, compute_speed_metrics
from exporter import validate_speed_uniqueness


def create_realistic_test_data() -> List[Dict]:
    """
    Create realistic test data with Section 2 information.
    Simulates multiple races with varied Section 2 data per dog.
    """
    data = [
        # Race 1 at SALE - 6 dogs with different Section 2 histories
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 1,
            'Runner': 'BLAZING STAR',
            'section2': '400m 23.15, 520m:29.50, 450m–24.80s'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 2,
            'Runner': 'QUICK SILVER',
            'section2': '400m 23.85, 520m:30.20'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 3,
            'Runner': 'STEADY EDDIE',
            'section2': '400m 24.50, 450m—26.10s'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 4,
            'Runner': 'FAST LANE',
            'section2': '520m:29.80, 450m 25.20'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 5,
            'Runner': 'TURBO BOOST',
            'section2': '400m 23.50, 520m:30.50, 450m–25.50s'
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 6,
            'Runner': 'SLOW MOTION',
            'section2': '400m 25.20, 450m 27.00'
        },
        
        # Race 2 at RICH - 5 dogs
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 1,
            'Runner': 'LIGHTNING BOLT',
            'section2': '520m:29.20, 450m 24.50, 400m 23.00'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 2,
            'Runner': 'THUNDER ROAD',
            'section2': '400m 23.60, 520m:30.80'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 3,
            'Runner': 'AVERAGE JOE',
            'section2': '450m 25.80, 400m 24.10'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 4,
            'Runner': 'SPEEDY GONZALES',
            'section2': '520m:29.60, 450m 24.90'
        },
        {
            'Track': 'RICH',
            'RaceNumber': 2,
            'Box': 5,
            'Runner': 'CRUISER',
            'section2': '400m 24.80, 450m 26.50'
        },
        
        # Race 3 at GAWL - 4 dogs
        {
            'Track': 'GAWL',
            'RaceNumber': 3,
            'Box': 1,
            'Runner': 'ROCKET MAN',
            'section2': '400m 23.20, 520m:29.70, 450m–24.70s'
        },
        {
            'Track': 'GAWL',
            'RaceNumber': 3,
            'Box': 2,
            'Runner': 'JET STREAM',
            'section2': '520m:30.10, 450m 25.30'
        },
        {
            'Track': 'GAWL',
            'RaceNumber': 3,
            'Box': 3,
            'Runner': 'COMET TAIL',
            'section2': '400m 24.00, 450m—25.90s'
        },
        {
            'Track': 'GAWL',
            'RaceNumber': 3,
            'Box': 4,
            'Runner': 'METEOR SHOWER',
            'section2': '520m:31.20, 400m 24.50'
        },
        
        # Race 4 at CAPA - 3 dogs (one missing Section 2 data)
        {
            'Track': 'CAPA',
            'RaceNumber': 4,
            'Box': 1,
            'Runner': 'FLASH GORDON',
            'section2': '400m 23.40, 520m:29.90'
        },
        {
            'Track': 'CAPA',
            'RaceNumber': 4,
            'Box': 2,
            'Runner': 'SONIC BOOM',
            'section2': '450m 25.00, 400m 23.80'
        },
        {
            'Track': 'CAPA',
            'RaceNumber': 4,
            'Box': 3,
            'Runner': 'NO DATA DOG',
            'section2': ''  # Missing Section 2 data
        },
    ]
    
    return data


def calculate_coverage_stats(df: pd.DataFrame) -> Dict[str, float]:
    """Calculate Section 2 coverage statistics."""
    total_dogs = len(df)
    
    # Count dogs with Section 2 data
    has_s2_all_speeds = df['S2_AllSpeeds'].apply(lambda x: len(x) > 0 if isinstance(x, list) else False).sum()
    has_s2_distance = df['S2_1_Distance'].notna().sum()
    has_s2_time = df['S2_1_RaceTime'].notna().sum()
    has_speed_kmh = df['Speed_kmh'].notna().sum()
    
    return {
        'total_dogs': total_dogs,
        'S2_AllSpeeds_coverage': (has_s2_all_speeds / total_dogs * 100) if total_dogs > 0 else 0,
        'S2_1_Distance_coverage': (has_s2_distance / total_dogs * 100) if total_dogs > 0 else 0,
        'S2_1_RaceTime_coverage': (has_s2_time / total_dogs * 100) if total_dogs > 0 else 0,
        'Speed_kmh_coverage': (has_speed_kmh / total_dogs * 100) if total_dogs > 0 else 0,
    }


def main():
    """Run validation pipeline."""
    
    print("=" * 80)
    print("VALIDATION: Per-Dog Speed_kmh Implementation")
    print("=" * 80)
    print()
    
    # Step 1: Create test data
    print("Step 1: Creating realistic test data...")
    raw_data = create_realistic_test_data()
    print(f"  Created {len(raw_data)} dog entries across 4 races")
    print()
    
    # Step 2: Parse Section 2 data
    print("Step 2: Parsing Section 2 (recent runs) data...")
    parsed_data = parse_race_card(raw_data)
    print(f"  Parsed Section 2 for {len(parsed_data)} dogs")
    print()
    
    # Step 3: Build advanced features
    print("Step 3: Building advanced features (Speed_kmh)...")
    featured_data = process_race_features(parsed_data)
    print(f"  Added Speed_kmh for {len(featured_data)} dogs")
    print()
    
    # Step 4: Convert to DataFrame
    print("Step 4: Converting to DataFrame...")
    df = pd.DataFrame(featured_data)
    print(f"  DataFrame shape: {df.shape}")
    print()
    
    # Step 5: Calculate Section 2 coverage
    print("=" * 80)
    print("Step 5: Section 2 Coverage Statistics")
    print("=" * 80)
    coverage = calculate_coverage_stats(df)
    print(f"Total dogs: {coverage['total_dogs']}")
    print(f"S2_AllSpeeds coverage: {coverage['S2_AllSpeeds_coverage']:.1f}%")
    print(f"S2_1_Distance coverage: {coverage['S2_1_Distance_coverage']:.1f}%")
    print(f"S2_1_RaceTime coverage: {coverage['S2_1_RaceTime_coverage']:.1f}%")
    print(f"Speed_kmh coverage: {coverage['Speed_kmh_coverage']:.1f}%")
    print()
    
    # Step 6: Display per-dog Speed_kmh values
    print("=" * 80)
    print("Step 6: Per-Dog Speed_kmh Values by Race")
    print("=" * 80)
    display_cols = ['Track', 'RaceNumber', 'Box', 'Runner', 'Speed_kmh']
    print(df[display_cols].to_string(index=False))
    print()
    
    # Step 7: Validate uniqueness per race
    print("=" * 80)
    print("Step 7: Validation - Check for Duplicate Speeds per Race")
    print("=" * 80)
    stats = validate_speed_uniqueness(df, verbose=True)
    print()
    
    # Step 8: Summary
    print("=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total races: {stats['total_races']}")
    print(f"Races with unique speeds: {stats['races_with_unique_speeds']}")
    print(f"Races with duplicate speeds: {stats['races_with_duplicates']}")
    print(f"Races with missing speeds: {stats['races_with_missing_speeds']}")
    print()
    
    # Calculate percentages
    if stats['total_races'] > 0:
        pct_unique = (stats['races_with_unique_speeds'] / stats['total_races']) * 100
        pct_dup = (stats['races_with_duplicates'] / stats['total_races']) * 100
        print(f"Unique Speed_kmh per race: {pct_unique:.1f}%")
        print(f"Duplicate Speed_kmh per race: {pct_dup:.1f}%")
    print()
    
    # Success criteria check
    print("=" * 80)
    print("SUCCESS CRITERIA CHECK")
    print("=" * 80)
    
    s2_coverage_ok = coverage['Speed_kmh_coverage'] > 80
    unique_races_ok = stats['total_races'] > 0 and (stats['races_with_unique_speeds'] / stats['total_races']) > 0.90
    no_dups_ok = stats['races_with_duplicates'] == 0
    
    print(f"✓ Section 2 coverage >80%: {'PASS' if s2_coverage_ok else 'FAIL'} ({coverage['Speed_kmh_coverage']:.1f}%)")
    print(f"✓ >90% races have unique per-dog speeds: {'PASS' if unique_races_ok else 'FAIL'} ({(stats['races_with_unique_speeds'] / stats['total_races'] * 100) if stats['total_races'] > 0 else 0:.1f}%)")
    print(f"✓ No [S2][DUP] in validation logs: {'PASS' if no_dups_ok else 'FAIL'}")
    print()
    
    if s2_coverage_ok and unique_races_ok and no_dups_ok:
        print("🎉 ALL SUCCESS CRITERIA MET!")
    else:
        print("⚠️  Some criteria not met - see details above")
    
    print("=" * 80)
    
    # Step 9: Export to files
    output_csv = '/tmp/validation_output.csv'
    output_xlsx = '/tmp/validation_output.xlsx'
    
    print()
    print("Step 9: Exporting output files...")
    df.to_csv(output_csv, index=False)
    print(f"  ✓ Exported CSV: {output_csv}")
    
    try:
        df.to_excel(output_xlsx, index=False, engine='openpyxl')
        print(f"  ✓ Exported Excel: {output_xlsx}")
    except Exception as e:
        print(f"  ⚠️  Excel export failed: {e}")
    
    print()
    print("Validation complete!")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

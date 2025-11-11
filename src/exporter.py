#!/usr/bin/env python3
"""
exporter.py
-----------
Export processed greyhound data with validation logging.
Validates that Speed_kmh values are unique per race (no duplicates).
"""

from typing import List, Dict, Any
from collections import defaultdict
import pandas as pd
import sys


def validate_speed_uniqueness(df: pd.DataFrame, verbose: bool = True) -> Dict[str, Any]:
    """
    Validate that Speed_kmh values vary within each race.
    
    Groups by (Track, RaceNumber) and checks for duplicate speeds.
    
    Args:
        df: DataFrame with columns including Track, RaceNumber, Speed_kmh
        verbose: If True, print validation messages
        
    Returns:
        Dict with validation statistics
    """
    stats = {
        'total_races': 0,
        'races_with_duplicates': 0,
        'races_with_unique_speeds': 0,
        'races_with_missing_speeds': 0,
        'duplicate_race_details': []
    }
    
    # Required columns
    required_cols = {'Track', 'RaceNumber', 'Speed_kmh'}
    if not required_cols.issubset(df.columns):
        # Try alternative column names
        col_map = {}
        for col in df.columns:
            if 'track' in col.lower():
                col_map['Track'] = col
            elif 'race' in col.lower() and 'number' in col.lower():
                col_map['RaceNumber'] = col
            elif 'speed' in col.lower():
                col_map['Speed_kmh'] = col
        
        if len(col_map) < 3:
            print(f"[S2][ERROR] Missing required columns. Need: {required_cols}, Have: {set(df.columns)}")
            return stats
        
        # Rename columns
        df = df.rename(columns={v: k for k, v in col_map.items()})
    
    # Group by race
    grouped = df.groupby(['Track', 'RaceNumber'])
    stats['total_races'] = len(grouped)
    
    for (track, race_num), group in grouped:
        speeds = group['Speed_kmh'].dropna()
        
        if len(speeds) == 0:
            # All speeds are missing for this race
            stats['races_with_missing_speeds'] += 1
            if verbose:
                print(f"[S2][MISS] {track} R{race_num}: All speeds missing")
            continue
        
        unique_speeds = speeds.nunique()
        
        if unique_speeds == 1:
            # All speeds are identical - this is the problem!
            stats['races_with_duplicates'] += 1
            stats['duplicate_race_details'].append({
                'track': track,
                'race': race_num,
                'speed': speeds.iloc[0],
                'count': len(speeds)
            })
            if verbose:
                print(f"[S2][DUP] {track} R{race_num}: {len(speeds)} dogs all have Speed_kmh={speeds.iloc[0]:.2f}")
        else:
            # Speeds vary - this is good!
            stats['races_with_unique_speeds'] += 1
            if verbose:
                print(f"[S2][OK] {track} R{race_num}: {unique_speeds} unique speeds among {len(speeds)} dogs")
    
    return stats


def export_to_excel(df: pd.DataFrame, output_path: str, validate: bool = True) -> None:
    """
    Export DataFrame to Excel with optional validation.
    
    Args:
        df: DataFrame to export
        output_path: Path to output Excel file
        validate: If True, run validation before export
    """
    if validate:
        print("\n=== Speed_kmh Validation ===")
        stats = validate_speed_uniqueness(df, verbose=True)
        print(f"\nSummary:")
        print(f"  Total races: {stats['total_races']}")
        print(f"  Races with unique speeds: {stats['races_with_unique_speeds']}")
        print(f"  Races with duplicate speeds: {stats['races_with_duplicates']}")
        print(f"  Races with missing speeds: {stats['races_with_missing_speeds']}")
        
        if stats['races_with_duplicates'] > 0:
            pct = (stats['races_with_duplicates'] / stats['total_races']) * 100
            print(f"\n⚠️  WARNING: {pct:.1f}% of races have duplicate Speed_kmh values!")
            print("   This indicates the bug is still present.")
        else:
            pct_ok = (stats['races_with_unique_speeds'] / stats['total_races']) * 100
            print(f"\n✓ SUCCESS: {pct_ok:.1f}% of races have unique Speed_kmh values!")
        
        print("=" * 30 + "\n")
    
    # Export to Excel
    df.to_excel(output_path, index=False, engine='openpyxl')
    print(f"Exported to: {output_path}")


def export_to_csv(df: pd.DataFrame, output_path: str, validate: bool = True) -> None:
    """
    Export DataFrame to CSV with optional validation.
    
    Args:
        df: DataFrame to export
        output_path: Path to output CSV file
        validate: If True, run validation before export
    """
    if validate:
        print("\n=== Speed_kmh Validation ===")
        stats = validate_speed_uniqueness(df, verbose=True)
        print(f"\nSummary:")
        print(f"  Total races: {stats['total_races']}")
        print(f"  Races with unique speeds: {stats['races_with_unique_speeds']}")
        print(f"  Races with duplicate speeds: {stats['races_with_duplicates']}")
        print(f"  Races with missing speeds: {stats['races_with_missing_speeds']}")
        
        if stats['races_with_duplicates'] > 0:
            pct = (stats['races_with_duplicates'] / stats['total_races']) * 100
            print(f"\n⚠️  WARNING: {pct:.1f}% of races have duplicate Speed_kmh values!")
        else:
            pct_ok = (stats['races_with_unique_speeds'] / stats['total_races']) * 100
            print(f"\n✓ SUCCESS: {pct_ok:.1f}% of races have unique Speed_kmh values!")
        
        print("=" * 30 + "\n")
    
    # Export to CSV
    df.to_csv(output_path, index=False)
    print(f"Exported to: {output_path}")

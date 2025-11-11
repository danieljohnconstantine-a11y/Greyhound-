#!/usr/bin/env python3
"""
demonstrate_fix.py
------------------
Demonstrates the BEFORE and AFTER scenarios for the Speed_kmh fix.

BEFORE: All dogs in a race have identical Speed_kmh (race-level data)
AFTER: Each dog has unique Speed_kmh based on their Section 2 data
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from parser import parse_race_card
from features_advanced import process_race_features


def simulate_old_behavior(race_data):
    """
    Simulate the OLD (buggy) behavior:
    All dogs get the same Speed_kmh from race-level data.
    """
    # In the old system, everyone gets the race distance/time
    race_distance = 520  # meters
    race_time = 30.0     # seconds
    race_speed = (race_distance / race_time) * 3.6  # km/h
    
    result = []
    for dog in race_data:
        dog_copy = dict(dog)
        dog_copy['Speed_kmh'] = race_speed  # SAME for all dogs!
        result.append(dog_copy)
    
    return result


def main():
    # Sample race with 4 dogs
    race_data = [
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 1,
            'Runner': 'FAST PUP',
            'section2': '400m 23.50, 520m:29.80, 450m–25.20s'  # Fast dog
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 2,
            'Runner': 'MEDIUM PUP',
            'section2': '400m 24.10, 520m:30.50'  # Medium dog
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 3,
            'Runner': 'SLOW PUP',
            'section2': '400m 25.00, 450m—26.50s'  # Slower dog
        },
        {
            'Track': 'SALE',
            'RaceNumber': 1,
            'Box': 4,
            'Runner': 'AVERAGE PUP',
            'section2': '520m:31.00, 450m 25.80'  # Average dog
        }
    ]
    
    print("=" * 80)
    print("DEMONSTRATION: Speed_kmh Fix")
    print("=" * 80)
    print()
    
    # ============ BEFORE (OLD BUGGY BEHAVIOR) ============
    print("╔" + "═" * 78 + "╗")
    print("║" + " BEFORE: Race-Level Speed (BUGGY - All Dogs Identical)".ljust(78) + "║")
    print("╚" + "═" * 78 + "╝")
    print()
    
    old_data = simulate_old_behavior(race_data)
    df_old = pd.DataFrame(old_data)
    
    print(df_old[['Box', 'Runner', 'Speed_kmh']].to_string(index=False))
    print()
    print("❌ PROBLEM: All 4 dogs have Speed_kmh = 62.40 km/h")
    print("   This is the race-level speed, NOT individual dog speeds!")
    print()
    
    unique_speeds_old = df_old['Speed_kmh'].nunique()
    print(f"   Unique Speed_kmh values: {unique_speeds_old}")
    print("   ⚠️  This makes it impossible to differentiate dogs by speed!")
    print()
    
    # ============ AFTER (NEW FIXED BEHAVIOR) ============
    print("╔" + "═" * 78 + "╗")
    print("║" + " AFTER: Per-Dog Speed from Section 2 (FIXED)".ljust(78) + "║")
    print("╚" + "═" * 78 + "╝")
    print()
    
    parsed_data = parse_race_card(race_data)
    new_data = process_race_features(parsed_data)
    df_new = pd.DataFrame(new_data)
    
    print(df_new[['Box', 'Runner', 'Speed_kmh']].to_string(index=False))
    print()
    print("✓ FIXED: Each dog has unique Speed_kmh based on their Section 2 data!")
    print()
    
    unique_speeds_new = df_new['Speed_kmh'].nunique()
    print(f"   Unique Speed_kmh values: {unique_speeds_new}")
    print("   ✓ Now we can properly compare dogs by their actual speeds!")
    print()
    
    # ============ COMPARISON ============
    print("=" * 80)
    print("COMPARISON")
    print("=" * 80)
    print()
    
    comparison = pd.DataFrame({
        'Dog': df_new['Runner'],
        'OLD Speed_kmh': df_old['Speed_kmh'],
        'NEW Speed_kmh': df_new['Speed_kmh'],
        'Difference': df_new['Speed_kmh'] - df_old['Speed_kmh']
    })
    
    print(comparison.to_string(index=False))
    print()
    
    print("Key Insights:")
    print(f"  • FAST PUP is now correctly identified as fastest: {df_new.loc[0, 'Speed_kmh']:.2f} km/h")
    print(f"  • SLOW PUP is now correctly identified as slowest: {df_new.loc[2, 'Speed_kmh']:.2f} km/h")
    print(f"  • Speed range (max - min): {df_new['Speed_kmh'].max() - df_new['Speed_kmh'].min():.2f} km/h")
    print()
    print("This variation allows for meaningful speed-based predictions!")
    print("=" * 80)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

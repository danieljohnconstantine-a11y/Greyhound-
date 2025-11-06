#!/usr/bin/env python3
"""
Tests for race history parser speed metrics extraction.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.race_history_parser import extract_speed_metrics_from_history, extract_race_history_section


def test_speed_metrics_extraction():
    """Test extracting speed metrics from race history text."""
    race_history = [
        "7th of 8 12/10/2025 RICHMOND Margin 16.3 Lengths Distance 520m SOT G RST MDN Race LADBROKES SRM IN MULTIS BATTLERS MAIDEN Prize $1,790 API 0.07 Race Time 0:30.41 Sec Time 4.39 Sec Time Adj 0.05 BP 5 Odds 40 Trainer Danielle Swain",
        "5th of 8 8/10/2025 RICHMOND Margin 6.8 Lengths Distance 320m SOT G RST MDN Race BISTRO OPEN FOR DINNER MAIDEN Prize $1,790 API 0.11 Race Time 0:18.76 Sec Time 2.18 Sec Time Adj 0.01 BP 4 Odds 80",
        "6th of 7 5/10/2025 RICHMOND Margin 8.8 Lengths Distance 320m SOT G RST MDN Race LADBROKES SRM IN MULTIS MAIDEN Prize $1,790 API 0.03 Race Time 0:18.43 Sec Time 2.13 BP 6 Odds 20",
        "Trial 99th 1/10/2025 RICH 401m 25.6 Race Time 0:23.08 Trainer DANIELLE SWAIN BP 1",
        "3rd of 8 26/09/2025 RICHMOND Margin 5.3 Lengths Distance 320m SOT G RST MDN Race LADBROKES QUICK MULTI MAIDEN Prize $1,790 API 0.05 Race Time 0:18.58 Sec Time 2.2 Sec Time Adj 0.03 BP 5 Odds 60",
    ]
    
    metrics = extract_speed_metrics_from_history(race_history)
    
    print("\n=== Speed Metrics Test ===")
    print(f"BestTime: {metrics['BestTime']}")
    print(f"SplitAvg: {metrics['SplitAvg']}")
    print(f"SpeedIndex: {metrics['SpeedIndex']}")
    print(f"EarlySpeed: {metrics['EarlySpeed']}")
    print(f"ClosingSpeed: {metrics['ClosingSpeed']}")
    
    # Validate that we got reasonable values
    assert metrics['BestTime'] is not None, "BestTime should be extracted"
    assert metrics['SpeedIndex'] is not None, "SpeedIndex should be calculated"
    assert float(metrics['SpeedIndex']) > 0, "SpeedIndex should be positive"
    
    print("\n✓ All tests passed!")


if __name__ == '__main__':
    test_speed_metrics_extraction()

#!/usr/bin/env python3
"""
test_exporter.py
----------------
Tests for exporter.py - Validation and export functionality
"""

import unittest
import sys
import os
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from exporter import validate_speed_uniqueness


class TestValidateSpeedUniqueness(unittest.TestCase):
    """Test validate_speed_uniqueness function"""
    
    def test_all_unique_speeds(self):
        """Test race with all unique speeds (good case)"""
        df = pd.DataFrame({
            'Track': ['SALE', 'SALE', 'SALE'],
            'RaceNumber': [1, 1, 1],
            'Speed_kmh': [60.0, 62.4, 61.2]
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        self.assertEqual(stats['total_races'], 1)
        self.assertEqual(stats['races_with_unique_speeds'], 1)
        self.assertEqual(stats['races_with_duplicates'], 0)
    
    def test_duplicate_speeds(self):
        """Test race with duplicate speeds (bad case)"""
        df = pd.DataFrame({
            'Track': ['SALE', 'SALE', 'SALE'],
            'RaceNumber': [1, 1, 1],
            'Speed_kmh': [60.0, 60.0, 60.0]
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        self.assertEqual(stats['total_races'], 1)
        self.assertEqual(stats['races_with_duplicates'], 1)
        self.assertEqual(stats['races_with_unique_speeds'], 0)
    
    def test_multiple_races_mixed(self):
        """Test multiple races with mixed results"""
        df = pd.DataFrame({
            'Track': ['SALE', 'SALE', 'SALE', 'RICH', 'RICH', 'RICH'],
            'RaceNumber': [1, 1, 1, 2, 2, 2],
            'Speed_kmh': [60.0, 62.0, 61.0, 58.0, 58.0, 58.0]
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        self.assertEqual(stats['total_races'], 2)
        self.assertEqual(stats['races_with_unique_speeds'], 1)  # SALE R1
        self.assertEqual(stats['races_with_duplicates'], 1)  # RICH R2
    
    def test_missing_speeds(self):
        """Test race with missing speeds"""
        df = pd.DataFrame({
            'Track': ['SALE', 'SALE', 'SALE'],
            'RaceNumber': [1, 1, 1],
            'Speed_kmh': [None, None, None]
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        self.assertEqual(stats['total_races'], 1)
        self.assertEqual(stats['races_with_missing_speeds'], 1)
    
    def test_partial_missing_speeds(self):
        """Test race with some missing speeds"""
        df = pd.DataFrame({
            'Track': ['SALE', 'SALE', 'SALE'],
            'RaceNumber': [1, 1, 1],
            'Speed_kmh': [60.0, None, 62.0]
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        # Should still count as unique since the non-null values differ
        self.assertEqual(stats['total_races'], 1)
        self.assertEqual(stats['races_with_unique_speeds'], 1)
    
    def test_empty_dataframe(self):
        """Test with empty dataframe"""
        df = pd.DataFrame({
            'Track': [],
            'RaceNumber': [],
            'Speed_kmh': []
        })
        
        stats = validate_speed_uniqueness(df, verbose=False)
        
        self.assertEqual(stats['total_races'], 0)


if __name__ == '__main__':
    unittest.main()

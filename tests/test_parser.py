#!/usr/bin/env python3
"""
test_parser.py
--------------
Tests for parser.py - Section 2 parsing and normalization
"""

import unittest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from parser import _extract_recent_runs, _normalize_section2, parse_dog_form


class TestExtractRecentRuns(unittest.TestCase):
    """Test _extract_recent_runs function"""
    
    def test_basic_format_with_space(self):
        """Test basic format: '400m 23.91'"""
        text = "400m 23.91"
        runs = _extract_recent_runs(text)
        
        self.assertEqual(len(runs), 1)
        self.assertAlmostEqual(runs[0]['distance'], 400.0)
        self.assertAlmostEqual(runs[0]['time'], 23.91)
        # Speed = (400 / 23.91) * 3.6 = 60.23 km/h
        self.assertAlmostEqual(runs[0]['speed'], 60.23, places=1)
    
    def test_format_with_colon(self):
        """Test format with colon: '520m:30.12'"""
        text = "520m:30.12"
        runs = _extract_recent_runs(text)
        
        self.assertEqual(len(runs), 1)
        self.assertAlmostEqual(runs[0]['distance'], 520.0)
        self.assertAlmostEqual(runs[0]['time'], 30.12)
        self.assertAlmostEqual(runs[0]['speed'], (520 / 30.12) * 3.6, places=2)
    
    def test_format_with_endash(self):
        """Test format with en-dash: '520m–29.94s'"""
        text = "520m–29.94s"
        runs = _extract_recent_runs(text)
        
        self.assertEqual(len(runs), 1)
        self.assertAlmostEqual(runs[0]['distance'], 520.0)
        self.assertAlmostEqual(runs[0]['time'], 29.94)
    
    def test_format_with_emdash(self):
        """Test format with em-dash: '520m—29.94'"""
        text = "520m—29.94"
        runs = _extract_recent_runs(text)
        
        self.assertEqual(len(runs), 1)
        self.assertAlmostEqual(runs[0]['distance'], 520.0)
        self.assertAlmostEqual(runs[0]['time'], 29.94)
    
    def test_multiple_runs(self):
        """Test multiple runs in one text"""
        text = "400m 23.91, 520m:30.12, 450m–25.50s"
        runs = _extract_recent_runs(text)
        
        self.assertEqual(len(runs), 3)
        self.assertAlmostEqual(runs[0]['distance'], 400.0)
        self.assertAlmostEqual(runs[1]['distance'], 520.0)
        self.assertAlmostEqual(runs[2]['distance'], 450.0)
    
    def test_invalid_distance_range(self):
        """Test that out-of-range distances are rejected"""
        # Too short
        text1 = "100m 15.00"
        runs1 = _extract_recent_runs(text1)
        self.assertEqual(len(runs1), 0)
        
        # Too long
        text2 = "900m 45.00"
        runs2 = _extract_recent_runs(text2)
        self.assertEqual(len(runs2), 0)
    
    def test_invalid_time_range(self):
        """Test that out-of-range times are rejected"""
        # Too fast
        text1 = "400m 10.00"
        runs1 = _extract_recent_runs(text1)
        self.assertEqual(len(runs1), 0)
        
        # Too slow
        text2 = "400m 65.00"
        runs2 = _extract_recent_runs(text2)
        self.assertEqual(len(runs2), 0)
    
    def test_empty_text(self):
        """Test with empty text"""
        runs = _extract_recent_runs("")
        self.assertEqual(len(runs), 0)
        
        runs = _extract_recent_runs(None)
        self.assertEqual(len(runs), 0)


class TestNormalizeSection2(unittest.TestCase):
    """Test _normalize_section2 function"""
    
    def test_single_run(self):
        """Test normalization with single run"""
        runs = [{'distance': 400.0, 'time': 23.91, 'speed': 60.23}]
        result = _normalize_section2(runs)
        
        self.assertEqual(result['S2_AllSpeeds'], [60.23])
        self.assertEqual(result['S2_1_Distance'], 400.0)
        self.assertEqual(result['S2_1_RaceTime'], 23.91)
        self.assertEqual(result['S2_1_Speed'], 60.23)
    
    def test_multiple_runs_fastest_selected(self):
        """Test that fastest run is selected"""
        runs = [
            {'distance': 400.0, 'time': 24.00, 'speed': 60.0},
            {'distance': 520.0, 'time': 30.00, 'speed': 62.4},  # Fastest
            {'distance': 450.0, 'time': 26.00, 'speed': 62.3}
        ]
        result = _normalize_section2(runs)
        
        self.assertEqual(len(result['S2_AllSpeeds']), 3)
        self.assertEqual(result['S2_1_Distance'], 520.0)  # Fastest run
        self.assertEqual(result['S2_1_Speed'], 62.4)
    
    def test_empty_runs(self):
        """Test with empty runs list"""
        result = _normalize_section2([])
        
        self.assertEqual(result['S2_AllSpeeds'], [])
        self.assertIsNone(result['S2_1_Distance'])
        self.assertIsNone(result['S2_1_RaceTime'])
        self.assertIsNone(result['S2_1_Speed'])


class TestParseDogForm(unittest.TestCase):
    """Test parse_dog_form function"""
    
    def test_parse_with_section2(self):
        """Test parsing dog form with Section 2 data"""
        form_data = {
            'dog_name': 'FAST PUP',
            'box': 1,
            'section2': '400m 23.91, 520m:30.12'
        }
        
        result = parse_dog_form(form_data)
        
        # Original data preserved
        self.assertEqual(result['dog_name'], 'FAST PUP')
        self.assertEqual(result['box'], 1)
        
        # Section 2 data added
        self.assertEqual(len(result['S2_AllSpeeds']), 2)
        self.assertIsNotNone(result['S2_1_Distance'])
        self.assertIsNotNone(result['S2_1_Speed'])
    
    def test_parse_without_section2(self):
        """Test parsing dog form without Section 2 data"""
        form_data = {
            'dog_name': 'SLOW PUP',
            'box': 2
        }
        
        result = parse_dog_form(form_data)
        
        # Original data preserved
        self.assertEqual(result['dog_name'], 'SLOW PUP')
        
        # Empty Section 2 data
        self.assertEqual(result['S2_AllSpeeds'], [])
        self.assertIsNone(result['S2_1_Speed'])


if __name__ == '__main__':
    unittest.main()

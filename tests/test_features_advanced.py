#!/usr/bin/env python3
"""
test_features_advanced.py
-------------------------
Tests for features_advanced.py - Advanced feature engineering
"""

import unittest
import sys
import os
import math

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from features_advanced import calculate_speed_kmh, compute_speed_metrics, build_advanced_features


class TestComputeSpeedMetrics(unittest.TestCase):
    """Test compute_speed_metrics function"""
    
    def test_with_valid_speeds(self):
        """Test with valid S2_AllSpeeds"""
        dog_data = {
            'S2_AllSpeeds': [60.0, 62.4, 61.2]
        }
        
        speed = compute_speed_metrics(dog_data)
        self.assertEqual(speed, 62.4)  # Max speed
    
    def test_with_single_speed(self):
        """Test with single speed"""
        dog_data = {
            'S2_AllSpeeds': [58.5]
        }
        
        speed = compute_speed_metrics(dog_data)
        self.assertEqual(speed, 58.5)
    
    def test_with_empty_speeds(self):
        """Test with empty S2_AllSpeeds returns NaN"""
        dog_data = {
            'S2_AllSpeeds': []
        }
        
        speed = compute_speed_metrics(dog_data)
        self.assertTrue(math.isnan(speed))
    
    def test_without_s2_field(self):
        """Test with missing S2_AllSpeeds field returns NaN"""
        dog_data = {}
        
        speed = compute_speed_metrics(dog_data)
        self.assertTrue(math.isnan(speed))
    
    def test_filters_invalid_values(self):
        """Test that NaN and invalid values are filtered"""
        dog_data = {
            'S2_AllSpeeds': [60.0, float('nan'), 62.4, 0, -5, 61.2]
        }
        
        speed = compute_speed_metrics(dog_data)
        self.assertEqual(speed, 62.4)  # Max of valid speeds
    
    def test_all_invalid_values(self):
        """Test with all invalid values returns NaN"""
        dog_data = {
            'S2_AllSpeeds': [float('nan'), 0, -5]
        }
        
        speed = compute_speed_metrics(dog_data)
        self.assertTrue(math.isnan(speed))


class TestCalculateSpeedKmh(unittest.TestCase):
    """Test calculate_speed_kmh function (legacy wrapper)"""
    
    def test_with_valid_speeds(self):
        """Test with valid S2_AllSpeeds"""
        dog_data = {
            'S2_AllSpeeds': [60.0, 62.4, 61.2]
        }
        
        speed = calculate_speed_kmh(dog_data)
        self.assertEqual(speed, 62.4)  # Max speed
    
    def test_with_single_speed(self):
        """Test with single speed"""
        dog_data = {
            'S2_AllSpeeds': [58.5]
        }
        
        speed = calculate_speed_kmh(dog_data)
        self.assertEqual(speed, 58.5)
    
    def test_with_empty_speeds(self):
        """Test with empty S2_AllSpeeds"""
        dog_data = {
            'S2_AllSpeeds': []
        }
        
        speed = calculate_speed_kmh(dog_data)
        self.assertIsNone(speed)
    
    def test_without_s2_field(self):
        """Test with missing S2_AllSpeeds field"""
        dog_data = {}
        
        speed = calculate_speed_kmh(dog_data)
        self.assertIsNone(speed)
    
    def test_filters_invalid_values(self):
        """Test that NaN and invalid values are filtered"""
        dog_data = {
            'S2_AllSpeeds': [60.0, float('nan'), 62.4, 0, -5, 61.2]
        }
        
        speed = calculate_speed_kmh(dog_data)
        self.assertEqual(speed, 62.4)  # Max of valid speeds
    
    def test_all_invalid_values(self):
        """Test with all invalid values"""
        dog_data = {
            'S2_AllSpeeds': [float('nan'), 0, -5]
        }
        
        speed = calculate_speed_kmh(dog_data)
        self.assertIsNone(speed)


class TestBuildAdvancedFeatures(unittest.TestCase):
    """Test build_advanced_features function"""
    
    def test_basic_feature_building(self):
        """Test that Speed_kmh is added correctly"""
        dog_data = {
            'dog_name': 'FAST PUP',
            'box': 1,
            'S2_AllSpeeds': [60.0, 62.4, 61.2]
        }
        
        result = build_advanced_features(dog_data)
        
        # Original data preserved
        self.assertEqual(result['dog_name'], 'FAST PUP')
        self.assertEqual(result['box'], 1)
        
        # Speed_kmh added
        self.assertEqual(result['Speed_kmh'], 62.4)
    
    def test_without_section2_data(self):
        """Test that Speed_kmh is NaN without Section 2 data"""
        dog_data = {
            'dog_name': 'SLOW PUP',
            'box': 2
        }
        
        result = build_advanced_features(dog_data)
        
        # Original data preserved
        self.assertEqual(result['dog_name'], 'SLOW PUP')
        
        # Speed_kmh is NaN
        self.assertTrue(math.isnan(result['Speed_kmh']))


if __name__ == '__main__':
    unittest.main()

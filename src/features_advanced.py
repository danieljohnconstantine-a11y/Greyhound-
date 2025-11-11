#!/usr/bin/env python3
"""
features_advanced.py
--------------------
Advanced feature engineering for greyhound racing predictions.
Focuses on per-dog metrics, especially Speed_kmh from Section 2 data.
"""

from typing import List, Dict, Optional
import math


def compute_speed_metrics(dog_data: Dict[str, any]) -> Optional[float]:
    """
    Compute Speed_kmh from Section 2 data (S2_AllSpeeds).
    
    Speed_kmh = max(S2_AllSpeeds) if list exists else NaN
    
    Uses the maximum speed from all parsed Section 2 runs.
    Does NOT fall back to RaceTime or CurrentDistance.
    
    Args:
        dog_data: Dict containing parsed form data with S2_AllSpeeds
        
    Returns:
        Max speed in km/h, or float('nan') if no valid Section 2 data
    """
    s2_speeds = dog_data.get('S2_AllSpeeds', [])
    
    if s2_speeds and len(s2_speeds) > 0:
        # Filter out any NaN or invalid values
        valid_speeds = [s for s in s2_speeds if s and not math.isnan(s) and s > 0]
        if valid_speeds:
            return max(valid_speeds)
    
    # Return NaN if Section 2 data unavailable (no fallback to RaceTime or CurrentDistance)
    return float('nan')


def calculate_speed_kmh(dog_data: Dict[str, any]) -> Optional[float]:
    """
    Legacy wrapper for compute_speed_metrics.
    
    Args:
        dog_data: Dict containing parsed form data with S2_AllSpeeds
        
    Returns:
        Max speed in km/h, or None if no valid Section 2 data
    """
    result = compute_speed_metrics(dog_data)
    return None if math.isnan(result) else result


def build_advanced_features(dog_data: Dict[str, any]) -> Dict[str, any]:
    """
    Build advanced features for a single dog.
    
    Args:
        dog_data: Dict containing parsed form data
        
    Returns:
        Enhanced dict with Speed_kmh and other advanced features
    """
    result = dict(dog_data)
    
    # Calculate Speed_kmh from Section 2 using compute_speed_metrics
    result['Speed_kmh'] = compute_speed_metrics(dog_data)
    
    # Additional advanced features can be added here
    # For now, focus on Speed_kmh as per requirements
    
    return result


def process_race_features(race_card: List[Dict[str, any]]) -> List[Dict[str, any]]:
    """
    Process advanced features for all dogs in a race.
    
    Args:
        race_card: List of dicts, each representing one dog's parsed form data
        
    Returns:
        List of dicts with advanced features added
    """
    return [build_advanced_features(dog) for dog in race_card]

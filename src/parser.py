#!/usr/bin/env python3
"""
parser.py
---------
Parse greyhound form data with Section 2 (recent runs) extraction.
Extracts per-dog distance/time pairs to calculate individual speeds.
"""

import re
from typing import List, Dict, Optional, Tuple


def _extract_recent_runs(section2_text: str) -> List[Dict[str, float]]:
    """
    Parse all distance/time pairs from Section 2 text.
    
    Handles formats like:
    - "400m 23.91"
    - "520m:30.12"
    - "520m–29.94s"
    - "520m—29.94"
    
    Args:
        section2_text: Raw text containing recent run data
        
    Returns:
        List of dicts with keys: distance, time, speed
        Each speed is calculated as (distance / time) × 3.6 km/h
    """
    runs = []
    
    if not section2_text:
        return runs
    
    # Pattern to match distance (200-800m) and time (15-60s)
    # Handles various separators: space, :, –, —, -
    # Optional 's' suffix on time
    pattern = r'(\d{3})m[\s:–—\-]+(\d{2}\.?\d{0,2})s?'
    
    matches = re.finditer(pattern, section2_text)
    
    for match in matches:
        try:
            distance = float(match.group(1))  # in meters
            time_str = match.group(2)
            
            # Normalize time string (handle missing decimal point)
            if '.' not in time_str and len(time_str) >= 4:
                # e.g., "2391" -> "23.91"
                time_str = time_str[:-2] + '.' + time_str[-2:]
            
            time = float(time_str)  # in seconds
            
            # Validate ranges
            if 200 <= distance <= 800 and 15 <= time <= 60:
                # Calculate speed in km/h: (distance_m / time_s) * 3.6
                speed = (distance / time) * 3.6
                
                runs.append({
                    'distance': distance,
                    'time': time,
                    'speed': speed
                })
        except (ValueError, IndexError):
            # Skip invalid entries
            continue
    
    return runs


def _normalize_section2(runs: List[Dict[str, float]]) -> Dict[str, any]:
    """
    Build normalized Section 2 data from parsed runs.
    
    Args:
        runs: List of run dicts from _extract_recent_runs
        
    Returns:
        Dict with:
        - S2_AllSpeeds: List of all speeds
        - S2_1_Distance: Distance of fastest run
        - S2_1_RaceTime: Time of fastest run
        - S2_1_Speed: Speed of fastest run
    """
    result = {
        'S2_AllSpeeds': [],
        'S2_1_Distance': None,
        'S2_1_RaceTime': None,
        'S2_1_Speed': None
    }
    
    if not runs:
        return result
    
    # Build list of all speeds
    result['S2_AllSpeeds'] = [r['speed'] for r in runs]
    
    # Find fastest run (max speed)
    fastest = max(runs, key=lambda r: r['speed'])
    result['S2_1_Distance'] = fastest['distance']
    result['S2_1_RaceTime'] = fastest['time']
    result['S2_1_Speed'] = fastest['speed']
    
    return result


def parse_dog_form(form_data: Dict[str, any]) -> Dict[str, any]:
    """
    Parse complete form data for a single dog.
    
    Args:
        form_data: Dict containing raw form data including 'section2' text
        
    Returns:
        Enhanced form_data dict with Section 2 fields added
    """
    result = dict(form_data)
    
    # Extract Section 2 text
    section2_text = form_data.get('section2', '') or form_data.get('Section2', '')
    
    # Parse recent runs
    runs = _extract_recent_runs(section2_text)
    
    # Normalize and add to result
    s2_data = _normalize_section2(runs)
    result.update(s2_data)
    
    return result


def parse_race_card(race_card: List[Dict[str, any]]) -> List[Dict[str, any]]:
    """
    Parse form data for all dogs in a race.
    
    Args:
        race_card: List of dicts, each representing one dog's form data
        
    Returns:
        List of enhanced dicts with Section 2 fields added
    """
    return [parse_dog_form(dog) for dog in race_card]

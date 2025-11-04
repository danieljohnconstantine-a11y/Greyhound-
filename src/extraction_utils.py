#!/usr/bin/env python3
"""
Enhanced extraction utilities for greyhound form parsing.
Implements robust patterns for Race, Box, Distance, and timing data extraction.
"""

import re
import logging
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)

# Robust regex patterns
RACE_PATTERNS = [
    r'(?:RACE|Race)\s+(?:No\.?|Number)?\s*(\d+)',  # "Race 6", "RACE No 6", "Race Number 6"
    r'(?:Broken\s+Hill\s+)?Race\s+(\d+)',  # "Broken Hill Race 6", "Race 6"
    r'RACE\s+(\d+)',  # "RACE 6"
]

BOX_PATTERNS = [
    r'^(\d+)\.',  # Leading ordinal: "1."
    r'^\[(\d+)\]',  # Bracketed: "[1]"
    r'^\((\d+)\)',  # Parenthesized: "(1)"
    r'^(\d+)\s+[A-Z]',  # Number followed by capital letter (dog name start)
]

DISTANCE_PATTERN = r'\b(\d{3,4})\s*m\b'  # "520m", "320 m", "401m"

# Race time patterns - accept various formats
RACE_TIME_PATTERNS = [
    r'Race\s+Time\s+(\d+):(\d+\.\d+)',  # "Race Time 0:30.41"
    r'Race\s+Time\s+(\d+\.\d+)',  # "Race Time 30.41"
    r'Time\s+(\d+):(\d+\.\d+)',  # "Time 0:30.41"
    r'Time\s+(\d+\.\d+)',  # "Time 30.41"
]

# Sectional time patterns
SECTIONAL_PATTERNS = [
    r'Sec\s+Time\s+(\d+\.\d+)',  # "Sec Time 4.39"
    r'First\s+Split\s+(\d+\.\d+)',  # "First Split 6.80"
    r'Sectional\s*1[:\s]*(\d+\.\d+)',  # "Sectional 1: 4.39" or "Sectional1 4.39"
    r'Sectional\s*2[:\s]*(\d+\.\d+)',  # "Sectional 2: 5.12"
    r'Sectional\s*3[:\s]*(\d+\.\d+)',  # "Sectional 3: 6.05"
    r'Sec\s+Time\s+Adj\s+(\d+\.\d+)',  # "Sec Time Adj 0.05"
]


def extract_race_number(text: str) -> Optional[int]:
    """
    Extract race number from text using content-first approach.
    Tries multiple patterns in order of specificity.
    """
    for pattern in RACE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                race_num = int(match.group(1))
                if 1 <= race_num <= 20:  # Sanity check
                    logger.debug(f"Extracted race number: {race_num} from pattern: {pattern}")
                    return race_num
            except (ValueError, IndexError):
                continue
    return None


def extract_box_number(text: str) -> Optional[int]:
    """
    Extract box number from dog line.
    Tries patterns in order: leading ordinal, bracketed, parenthesized.
    """
    # Try each pattern
    for pattern in BOX_PATTERNS:
        match = re.match(pattern, text.strip())
        if match:
            try:
                box_num = int(match.group(1))
                if 1 <= box_num <= 10:  # Valid box range
                    logger.debug(f"Extracted box number: {box_num} from pattern: {pattern}")
                    return box_num
            except (ValueError, IndexError):
                continue
    
    logger.warning(f"No valid box number found in: {text[:50]}")
    return None


def extract_distance(text: str) -> Optional[int]:
    """Extract distance in meters from text"""
    match = re.search(DISTANCE_PATTERN, text)
    if match:
        try:
            distance = int(match.group(1))
            if 200 <= distance <= 1200:  # Sanity check for greyhound race distances
                return distance
        except ValueError:
            pass
    return None


def normalize_race_time(time_str: str) -> Optional[float]:
    """
    Normalize race time to seconds (float).
    Accepts: "0:30.41", "30.41", "0:18.76", "18.76"
    Returns: seconds as float
    """
    # Try colon format first (MM:SS.ss)
    colon_match = re.match(r'(\d+):(\d+\.\d+)', time_str.strip())
    if colon_match:
        try:
            minutes = int(colon_match.group(1))
            seconds = float(colon_match.group(2))
            total_seconds = minutes * 60 + seconds
            return total_seconds
        except ValueError:
            pass
    
    # Try direct seconds format
    try:
        seconds = float(time_str.strip())
        # If it's already in seconds (reasonable range for greyhound races)
        if 10.0 <= seconds <= 120.0:
            return seconds
    except ValueError:
        pass
    
    return None


def extract_race_time(text: str) -> Optional[float]:
    """Extract and normalize race time from text"""
    for pattern in RACE_TIME_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:
                # Format: MM:SS.ss
                time_str = f"{match.group(1)}:{match.group(2)}"
            else:
                # Format: SS.ss or direct seconds
                time_str = match.group(1)
            
            normalized = normalize_race_time(time_str)
            if normalized:
                logger.debug(f"Extracted race time: {normalized}s from: {time_str}")
                return normalized
    
    return None


def extract_sectionals(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Extract sectional times from text.
    Returns tuple: (Sectional1, Sectional2, Sectional3)
    """
    sectional1 = None
    sectional2 = None
    sectional3 = None
    
    # Try specific sectional patterns
    for pattern in SECTIONAL_PATTERNS:
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        if matches:
            for i, match in enumerate(matches[:3]):  # Take first 3 sectionals
                try:
                    value = float(match.group(1))
                    if 0.5 <= value <= 30.0:  # Reasonable range for sectionals
                        if i == 0 and sectional1 is None:
                            sectional1 = value
                        elif i == 1 and sectional2 is None:
                            sectional2 = value
                        elif i == 2 and sectional3 is None:
                            sectional3 = value
                except (ValueError, IndexError):
                    continue
    
    if sectional1 or sectional2 or sectional3:
        logger.debug(f"Extracted sectionals: {sectional1}, {sectional2}, {sectional3}")
    
    return sectional1, sectional2, sectional3


def calculate_speed_metrics(distance: Optional[int], race_time: Optional[float], 
                            sectional1: Optional[float] = None,
                            sectional2: Optional[float] = None,
                            sectional3: Optional[float] = None) -> dict:
    """
    Calculate distance-aware speed metrics.
    Returns dict with: SpeedIndex, EarlySpeed, ClosingSpeed, SplitAvg
    """
    metrics = {
        'SpeedIndex': None,
        'EarlySpeed': None,
        'ClosingSpeed': None,
        'SplitAvg': None,
    }
    
    # Calculate SpeedIndex (meters per second)
    if distance and race_time and race_time > 0:
        metrics['SpeedIndex'] = round(distance / race_time, 2)
    
    # Calculate SplitAvg
    sectionals = [s for s in [sectional1, sectional2, sectional3] if s is not None]
    if sectionals:
        metrics['SplitAvg'] = round(sum(sectionals) / len(sectionals), 2)
    
    # Calculate EarlySpeed (from first sectional if distance >= 400m)
    if distance and distance >= 400 and sectional1:
        # Assume first sectional is ~200m
        metrics['EarlySpeed'] = round(200.0 / sectional1, 2) if sectional1 > 0 else None
    
    # Calculate ClosingSpeed (overall speed, same as SpeedIndex for now)
    if metrics['SpeedIndex']:
        metrics['ClosingSpeed'] = metrics['SpeedIndex']
    
    return metrics


def parse_race_history_line(line: str) -> dict:
    """
    Parse a race history line for distance, times, and sectionals.
    Returns dict with all extracted values.
    """
    result = {
        'Distance': extract_distance(line),
        'RaceTime': extract_race_time(line),
        'Sectional1': None,
        'Sectional2': None,
        'Sectional3': None,
    }
    
    # Extract sectionals
    sect1, sect2, sect3 = extract_sectionals(line)
    result['Sectional1'] = sect1
    result['Sectional2'] = sect2
    result['Sectional3'] = sect3
    
    # Calculate speed metrics
    if result['Distance'] and result['RaceTime']:
        metrics = calculate_speed_metrics(
            result['Distance'], 
            result['RaceTime'],
            sect1, sect2, sect3
        )
        result.update(metrics)
    
    return result

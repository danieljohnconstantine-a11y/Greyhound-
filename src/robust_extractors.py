"""
Robust extraction patterns for race form parsing
Implements flexible patterns to handle multiple PDF formats
"""

import re
import logging
from typing import Optional, Tuple, Dict, List

logger = logging.getLogger(__name__)

# ===== RACE NUMBER EXTRACTION =====

RACE_PATTERNS = [
    r'(?:RACE|Race|race)\s+(?:NO|No|no|NUMBER|Number)?\s*[:\.]?\s*(\d+)',  # "Race 6", "Race No 6", "RACE 6"
    r'(?:Broken\s+Hill|BROKEN\s+HILL)\s+(?:RACE|Race)\s+(\d+)',  # "Broken Hill Race 6"
    r'^(\d+)\s*\.\s*(?:RACE|Race)',  # "6. RACE"
]

def extract_race_number(text: str, context_lines: List[str] = None) -> Optional[int]:
    """
    Extract race number from text with multiple pattern support
    
    Args:
        text: Text to search
        context_lines: Additional context lines to search
        
    Returns:
        Race number as int, or None if not found
    """
    # Try each pattern on main text
    for pattern in RACE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                race_num = int(match.group(1))
                if 1 <= race_num <= 20:  # Sanity check
                    logger.debug(f"Extracted race number {race_num} from: {text[:50]}...")
                    return race_num
            except (ValueError, IndexError):
                continue
    
    # Try context lines if provided
    if context_lines:
        for line in context_lines:
            for pattern in RACE_PATTERNS:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    try:
                        race_num = int(match.group(1))
                        if 1 <= race_num <= 20:
                            logger.debug(f"Extracted race number {race_num} from context: {line[:50]}...")
                            return race_num
                    except (ValueError, IndexError):
                        continue
    
    return None


# ===== BOX NUMBER EXTRACTION =====

BOX_PATTERNS = [
    r'^(\d{1,2})\.\s+',  # "1. " at start of line
    r'^\[(\d{1,2})\]',  # "[1]" at start
    r'^\((\d{1,2})\)',  # "(1)" at start
    r'^(\d{1,2})\s+[A-Z]',  # "1 DOG" at start
]

def extract_box_number(line: str) -> Optional[int]:
    """
    Extract box number from dog line with multiple pattern support
    
    Args:
        line: Line containing dog information
        
    Returns:
        Box number as int (1-10), or None if not found/invalid
    """
    # Try each pattern
    for pattern in BOX_PATTERNS:
        match = re.match(pattern, line.strip())
        if match:
            try:
                box_num = int(match.group(1))
                if 1 <= box_num <= 10:
                    logger.debug(f"Extracted box {box_num} from: {line[:50]}...")
                    return box_num
                else:
                    logger.warning(f"Box number {box_num} out of range (1-10) in: {line[:50]}...")
            except (ValueError, IndexError):
                continue
    
    # Log if no valid box found
    logger.warning(f"No valid box number found in: {line[:50]}...")
    return None


# ===== DISTANCE EXTRACTION =====

DISTANCE_PATTERN = r'\b(\d{3,4})\s*m\b'

def extract_distance(text: str) -> Optional[int]:
    """
    Extract distance in meters
    
    Args:
        text: Text to search
        
    Returns:
        Distance in meters as int, or None
    """
    match = re.search(DISTANCE_PATTERN, text, re.IGNORECASE)
    if match:
        try:
            distance = int(match.group(1))
            if 200 <= distance <= 1000:  # Sanity check for greyhound races
                return distance
        except ValueError:
            pass
    return None


# ===== RACE TIME EXTRACTION =====

RACE_TIME_PATTERNS = [
    r'(?:Race\s+Time|RaceTime|TIME)[:\s]+(\d+):(\d+)\.(\d+)',  # "Race Time 0:30.41"
    r'(?:Race\s+Time|RaceTime|TIME)[:\s]+(\d+)\.(\d+)',  # "Race Time 30.41"
    r'\b(\d+):(\d+)\.(\d+)\b',  # General "0:30.41"
    r'\b(\d+)\.(\d+)\s*s\b',  # "30.41s"
]

def extract_race_time(text: str) -> Optional[float]:
    """
    Extract race time and normalize to seconds
    
    Args:
        text: Text to search
        
    Returns:
        Race time in seconds as float, or None
    """
    # Try pattern with minutes:seconds.milliseconds (0:30.41)
    match = re.search(r'(?:Race\s+Time|RaceTime|TIME)[:\s]+(\d+):(\d+)\.(\d+)', text, re.IGNORECASE)
    if match:
        try:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            milliseconds = int(match.group(3))
            total_seconds = minutes * 60 + seconds + milliseconds / 100.0
            if 0 < total_seconds < 120:  # Sanity check (< 2 minutes)
                return round(total_seconds, 2)
        except (ValueError, IndexError):
            pass
    
    # Try pattern with seconds only (30.41)
    match = re.search(r'(?:Race\s+Time|RaceTime|TIME)[:\s]+(\d+)\.(\d+)', text, re.IGNORECASE)
    if match:
        try:
            seconds = int(match.group(1))
            milliseconds = int(match.group(2))
            total_seconds = seconds + milliseconds / 100.0
            if 0 < total_seconds < 120:
                return round(total_seconds, 2)
        except (ValueError, IndexError):
            pass
    
    # Try general patterns
    match = re.search(r'\b(\d+):(\d+)\.(\d+)\b', text)
    if match:
        try:
            minutes = int(match.group(1))
            seconds = int(match.group(2))
            milliseconds = int(match.group(3))
            total_seconds = minutes * 60 + seconds + milliseconds / 100.0
            if 0 < total_seconds < 120:
                return round(total_seconds, 2)
        except (ValueError, IndexError):
            pass
    
    return None


# ===== SECTIONAL TIME EXTRACTION =====

SECTIONAL_PATTERNS = [
    r'(?:Sec\s+Time|SecTime|Sectional)[:\s]+(\d+)\.(\d+)',  # "Sec Time 4.39"
    r'(?:First\s+Split|1st\s+Split)[:\s]+(\d+)\.(\d+)',  # "First Split 6.80"
    r'(?:Sectional\s*1|Sect\s*1)[:\s]?(\d+)\.(\d+)',  # "Sectional 1: 4.39"
    r'(?:Sectional\s*2|Sect\s*2)[:\s]?(\d+)\.(\d+)',  # "Sectional 2: 3.12"
    r'(?:Sectional\s*3|Sect\s*3)[:\s]?(\d+)\.(\d+)',  # "Sectional 3: 2.98"
]

def extract_sectional_times(text: str) -> Dict[str, Optional[float]]:
    """
    Extract sectional times from text
    
    Args:
        text: Text to search
        
    Returns:
        Dictionary with Sectional1, Sectional2, Sectional3 keys
    """
    sectionals = {
        'Sectional1': None,
        'Sectional2': None,
        'Sectional3': None,
    }
    
    # General "Sec Time" - use as Sectional1
    match = re.search(r'(?:Sec\s+Time|SecTime)[:\s]+(\d+)\.(\d+)', text, re.IGNORECASE)
    if match:
        try:
            seconds = int(match.group(1))
            milliseconds = int(match.group(2))
            sectionals['Sectional1'] = round(seconds + milliseconds / 100.0, 2)
        except (ValueError, IndexError):
            pass
    
    # First Split
    match = re.search(r'(?:First\s+Split|1st\s+Split)[:\s]+(\d+)\.(\d+)', text, re.IGNORECASE)
    if match:
        try:
            seconds = int(match.group(1))
            milliseconds = int(match.group(2))
            if sectionals['Sectional1'] is None:
                sectionals['Sectional1'] = round(seconds + milliseconds / 100.0, 2)
        except (ValueError, IndexError):
            pass
    
    # Specific sectionals
    for i in range(1, 4):
        pattern = rf'(?:Sectional\s*{i}|Sect\s*{i})[:\s]?(\d+)\.(\d+)'
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                seconds = int(match.group(1))
                milliseconds = int(match.group(2))
                sectionals[f'Sectional{i}'] = round(seconds + milliseconds / 100.0, 2)
            except (ValueError, IndexError):
                pass
    
    return sectionals


# ===== SPEED INDEX CALCULATION =====

def calculate_speed_index(distance: Optional[int], race_time: Optional[float]) -> Optional[float]:
    """
    Calculate speed index in meters per second
    
    Args:
        distance: Distance in meters
        race_time: Race time in seconds
        
    Returns:
        Speed in m/s, or None if inputs invalid
    """
    if distance is not None and race_time is not None and race_time > 0:
        speed = distance / race_time
        return round(speed, 2)
    return None


def calculate_early_closing_speed(sectionals: Dict[str, Optional[float]], 
                                   speed_index: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """
    Calculate early and closing speed from sectionals and overall speed
    
    Args:
        sectionals: Dictionary of sectional times
        speed_index: Overall speed in m/s
        
    Returns:
        (early_speed, closing_speed) tuple
    """
    early_speed = None
    closing_speed = None
    
    # If we have sectional 1, use it for early speed (assume first 200m)
    if sectionals.get('Sectional1') is not None and sectionals['Sectional1'] > 0:
        # Assume first sectional is for 200m
        early_speed = round(200 / sectionals['Sectional1'], 2)
    
    # Use overall speed index as closing speed if available
    if speed_index is not None:
        closing_speed = speed_index
    
    return early_speed, closing_speed

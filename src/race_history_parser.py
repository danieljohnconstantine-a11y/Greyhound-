#!/usr/bin/env python3
"""
Race history parser for extracting speed metrics from historical race data.
Parses lines like: "7th of 8 12/10/2025 RICHMOND Margin 16.3 Lengths Distance 520m ... Race Time 0:30.41 Sec Time 4.39..."
"""

import re
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


def parse_race_history_line(line: str) -> Optional[Dict]:
    """
    Parse a single race history line to extract timing and distance data.
    
    Example line formats:
    - Old format: "7th of 8 12/10/2025 RICHMOND Margin 16.3 Lengths Distance 520m ... Race Time 0:30.41 Sec Time 4.39"
    - QLAKG format: "5/10/2025 CAPA 366m 1.87 Race Time 0:20.00 Sec Time 1.87 Margin 8.6 Lengths Position: 8"
    
    Returns dict with:
        - distance: int (meters)
        - race_time: str (e.g. "0:30.41")
        - race_time_seconds: float
        - sectional_time: float or None
        - sectional_adj: float or None
        - placement: str (e.g. "7th of 8")
        - margin: float or None
    """
    if not line or len(line) < 20:
        return None
    
    # Check if this looks like a race history line
    # Must have either: (a) placement AND distance, or (b) date AND distance AND race time
    has_old_placement = re.search(r'\d+(?:st|nd|rd|th)\s+of\s+\d+', line)
    has_date = re.search(r'\d{1,2}/\d{1,2}/\d{4}', line)
    has_distance = re.search(r'(\d{3,4})m', line)
    has_race_time = re.search(r'Race Time\s+([\d:\.]+)', line)
    
    if not ((has_old_placement and has_distance) or (has_date and has_distance and has_race_time)):
        return None
    
    result = {}
    
    # Extract placement (old format: "7th of 8", QLAKG format: "Position: 8")
    placement_match = re.search(r'(\d+)(?:st|nd|rd|th)\s+of\s+(\d+)', line)
    if placement_match:
        result['placement'] = placement_match.group(0)
        result['finish_position'] = int(placement_match.group(1))
        result['field_size'] = int(placement_match.group(2))
    else:
        # Try QLAKG format "Position: 8"
        position_match = re.search(r'Position:\s*(\d+)', line)
        if position_match:
            result['finish_position'] = int(position_match.group(1))
    
    # Extract distance (e.g., "Distance 520m" or just "366m")
    distance_match = re.search(r'(?:Distance\s+)?(\d{3,4})m', line)
    if distance_match:
        result['distance'] = int(distance_match.group(1))
    else:
        return None  # Must have distance to be useful
    
    # Extract margin (e.g., "Margin 16.3" or "Margin 8.6 Lengths")
    margin_match = re.search(r'Margin\s+([\d\.]+)', line)
    if margin_match:
        try:
            result['margin'] = float(margin_match.group(1))
        except ValueError:
            result['margin'] = None
    
    # Extract Race Time (e.g., "Race Time 0:30.41" or "Race Time 0:20.00")
    race_time_match = re.search(r'Race Time\s+([\d:\.]+)', line)
    if race_time_match:
        result['race_time'] = race_time_match.group(1)
        result['race_time_seconds'] = convert_time_to_seconds(race_time_match.group(1))
    else:
        return None  # Must have race time
    
    # Extract Sectional Time (e.g., "Sec Time 4.39")
    sec_time_match = re.search(r'Sec Time\s+([\d\.]+)', line)
    if sec_time_match:
        try:
            result['sectional_time'] = float(sec_time_match.group(1))
        except ValueError:
            result['sectional_time'] = None
    
    # Extract Sectional Time Adjusted (e.g., "Sec Time Adj 0.05")
    sec_adj_match = re.search(r'Sec Time Adj\s+([\d\.]+)', line)
    if sec_adj_match:
        try:
            result['sectional_adj'] = float(sec_adj_match.group(1))
        except ValueError:
            result['sectional_adj'] = None
    
    return result


def convert_time_to_seconds(time_str: str) -> float:
    """Convert time string like '0:30.41' or '18.76' to seconds"""
    try:
        if ':' in time_str:
            # Format: MM:SS.MS or M:SS.MS
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
        else:
            # Format: SS.MS
            return float(time_str)
    except (ValueError, IndexError):
        return 0.0
    return 0.0


def extract_speed_metrics_from_history(lines: List[str]) -> Dict:
    """
    Extract speed metrics from all race history lines for a dog.
    
    Returns dict with:
        - BestTime: fastest race time (as string MM:SS.MS)
        - SplitAvg: average sectional time
        - SpeedIndex: normalized speed (distance/time) for best race
        - EarlySpeed: average speed for early positions
        - ClosingSpeed: average speed for late positions
    """
    metrics = {
        'BestTime': None,
        'SplitAvg': None,
        'SpeedIndex': None,
        'EarlySpeed': None,
        'ClosingSpeed': None,
        'Sectional1': None,
        'Sectional2': None,
        'Sectional3': None,
    }
    
    history_entries = []
    sectional_times = []
    
    # Parse all race history lines
    for line in lines:
        entry = parse_race_history_line(line)
        if entry and entry.get('distance') and entry.get('race_time_seconds'):
            history_entries.append(entry)
            
            # Collect sectional times
            if entry.get('sectional_time'):
                sectional_times.append(entry['sectional_time'])
    
    if not history_entries:
        return metrics
    
    # Find best time (fastest speed = distance / time)
    best_entry = None
    best_speed = 0.0
    
    for entry in history_entries:
        speed = entry['distance'] / entry['race_time_seconds']
        if speed > best_speed:
            best_speed = speed
            best_entry = entry
    
    if best_entry:
        metrics['BestTime'] = best_entry['race_time']
        metrics['SpeedIndex'] = round(best_speed, 2)  # meters per second
    
    # Calculate average sectional time
    if sectional_times:
        metrics['SplitAvg'] = round(sum(sectional_times) / len(sectional_times), 2)
        
        # Populate individual sectionals if available
        if len(sectional_times) >= 1:
            metrics['Sectional1'] = sectional_times[0]
        if len(sectional_times) >= 2:
            metrics['Sectional2'] = sectional_times[1]
        if len(sectional_times) >= 3:
            metrics['Sectional3'] = sectional_times[2]
    
    # Calculate EarlySpeed and ClosingSpeed
    # EarlySpeed: races where dog finished in top 3
    # ClosingSpeed: based on margin and sectional data
    early_speeds = []
    closing_speeds = []
    
    for entry in history_entries:
        speed = entry['distance'] / entry['race_time_seconds']
        
        # EarlySpeed: speed in races where dog was competitive early
        if entry.get('finish_position', 99) <= 3:
            early_speeds.append(speed)
        
        # ClosingSpeed: speed in all races (represents finishing ability)
        closing_speeds.append(speed)
    
    if early_speeds:
        metrics['EarlySpeed'] = round(sum(early_speeds) / len(early_speeds), 2)
    
    if closing_speeds:
        metrics['ClosingSpeed'] = round(sum(closing_speeds) / len(closing_speeds), 2)
    
    return metrics


def extract_race_history_section(lines: List[str], dog_name: str, box_num: int) -> List[str]:
    """
    Extract the race history section for a specific dog.
    Looks for lines containing race results after the dog's summary line.
    """
    history_lines = []
    in_history = False
    lines_since_dog = 0
    
    for i, line in enumerate(lines):
        # Look for the dog's summary line (contains box number and dog name)
        if not in_history and str(box_num) in line and dog_name in line.upper():
            in_history = True
            lines_since_dog = 0
            continue
        
        if in_history:
            lines_since_dog += 1
            
            # Collect lines that look like race history
            # QLAKG format: has date (DD/MM/YYYY), track code, distance (XXXm), and "Race Time"
            # Example: "5/10/2025 CAPA 366m 1.87 Race Time 0:20.00 Sec Time 1.87 Margin 8.6"
            has_date = re.search(r'\d{1,2}/\d{1,2}/\d{4}', line)
            has_distance = re.search(r'\d{3,4}m', line)
            has_race_time = 'Race Time' in line or 'Sec Time' in line
            
            # Also match old format: "1st of 8" style
            has_placement = re.search(r'\d+(?:st|nd|rd|th)\s+of\s+\d+', line)
            
            if (has_date and has_distance) or (has_date and has_race_time) or has_placement:
                history_lines.append(line)
            
            # Stop after reasonable number of lines or when we hit next dog
            if lines_since_dog > 30:
                break
            
            # Stop if we hit next dog's summary (new box number at start of line)
            if re.match(r'^\d+\.\s+[A-Z]', line):
                break
    
    return history_lines

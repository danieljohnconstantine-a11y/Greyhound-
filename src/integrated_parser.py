#!/usr/bin/env python3
"""
Integrated parser with proper race context tracking per file.
Fixes the "all Race=7" issue by maintaining rolling race context within each file.
"""

import os
import re
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pdfplumber
import pandas as pd

logger = logging.getLogger(__name__)

# 62-column order
COLUMNS = [
    "Track", "Race", "Box", "DogName", "Trainer", "Grade", "Distance", "RaceDate",
    "Form", "WinRate", "PlaceRate", "Odds", "BestTime", "Margin",
    "Sectional1", "Sectional2", "Sectional3", "RaceComment",
    "Starts", "Wins", "Seconds", "Thirds", "CareerPrizeMoney", "CareerBest",
    "RaceClass", "TrackCondition", "Weather", "Interference",
    "TrainerWinRate", "TrainerState", "TrainerCity",
    "Owner", "Sire", "Dam", "Age", "Sex", "Color", "Weight",
    "LastStartDate", "LastStartTrack", "LastStartResult", "LastStartMargin", "LastStartComment",
    "SplitAvg", "SpeedIndex", "ConsistencyIndex",
    "TrackDistanceWins", "TrackDistancePlaces", "TrackStarts", "TrackWins",
    "DistanceStarts", "DistanceWins", "DistancePlaces",
    "BoxHistory", "BoxWins", "BoxPlaces",
    "EarlySpeed", "ClosingSpeed", "Score",
    "Comments", "Notes", "SourcePDF"
]


def extract_race_from_text(text: str) -> Optional[int]:
    """Extract race number from text with multiple format support."""
    patterns = [
        r'Race\s+No\.?\s*(\d+)',
        r'Race\s+(\d+)',
        r'RACE\s+(\d+)',
        r'Broken\s+Hill\s+Race\s+(\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                continue
    return None


def extract_box_from_line(line: str) -> Optional[int]:
    """Extract box number with precedence: ordinal > bracket > paren."""
    # Leading ordinal: "1."
    match = re.match(r'^\s*(\d{1,2})\.', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    # Bracketed: "[1]"
    match = re.match(r'^\s*\[(\d{1,2})\]', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    # Parenthesized: "(1)"
    match = re.match(r'^\s*\((\d{1,2})\)', line)
    if match:
        box = int(match.group(1))
        if 1 <= box <= 10:
            return box
    
    return None


def extract_distance_from_text(text: str) -> Optional[int]:
    """Extract distance in meters."""
    match = re.search(r'\b(\d{3,4})\s*m\b', text, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def normalize_race_time(time_str: str) -> Optional[float]:
    """
    Normalize race time to seconds.
    Accepts: "0:30.41", "30.41", "0:18.76", "18.76"
    """
    if not time_str:
        return None
    
    time_str = time_str.strip()
    
    # Format: "0:30.41" or "1:05.23"
    if ':' in time_str:
        parts = time_str.split(':')
        if len(parts) == 2:
            try:
                minutes = int(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
            except ValueError:
                pass
    
    # Format: "30.41" or "18.76"
    try:
        return float(time_str)
    except ValueError:
        return None


def extract_race_time_from_text(text: str) -> Optional[float]:
    """Extract race time and normalize to seconds."""
    match = re.search(r'Race\s+Time\s+(\d+:?\d*\.\d+)', text, re.IGNORECASE)
    if match:
        return normalize_race_time(match.group(1))
    return None


def extract_sectionals_from_text(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Extract up to 3 sectional times."""
    sectionals = [None, None, None]
    
    # Pattern: "Sec Time 4.39"
    matches = re.findall(r'Sec\s+Time\s+(\d+\.\d+)', text, re.IGNORECASE)
    for i, match in enumerate(matches[:3]):
        try:
            sectionals[i] = float(match)
        except ValueError:
            pass
    
    # Pattern: "First Split 6.80"
    if not sectionals[0]:
        match = re.search(r'First\s+Split\s+(\d+\.\d+)', text, re.IGNORECASE)
        if match:
            try:
                sectionals[0] = float(match.group(1))
            except ValueError:
                pass
    
    # Pattern: "Sectional 1: 4.39"
    for i in range(3):
        if not sectionals[i]:
            pattern = rf'Sectional\s*{i+1}[:\s]+(\d+\.\d+)'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    sectionals[i] = float(match.group(1))
                except ValueError:
                    pass
    
    return tuple(sectionals)


def parse_pdf_with_race_context(pdf_path: str) -> List[Dict]:
    """
    Parse PDF with rolling race context per file.
    Maintains current_race across pages and assigns to each dog row.
    """
    records = []
    current_race = None
    track_name = Path(pdf_path).stem.split('form')[0].upper()  # Extract track from filename
    
    logger.info(f"Parsing {pdf_path} for track {track_name}")
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text() or ""
                
                # Check for race header on this page
                race_num = extract_race_from_text(text)
                if race_num is not None:
                    current_race = race_num
                    logger.debug(f"Page {page_num}: Found race header -> Race {current_race}")
                
                # Extract dog lines
                lines = text.split('\n')
                for line in lines:
                    # Try to extract box number
                    box = extract_box_from_line(line)
                    if box is None:
                        continue
                    
                    # Extract dog name (text after box number)
                    dog_name_match = re.search(r'(?:^\s*\d{1,2}\.|\[\d{1,2}\]|\(\d{1,2}\))\s*([A-Za-z][A-Za-z\s\'\-]+)', line)
                    if not dog_name_match:
                        continue
                    
                    dog_name = dog_name_match.group(1).strip()
                    if not dog_name or len(dog_name) < 2:
                        continue
                    
                    # We have a valid dog row
                    record = {col: None for col in COLUMNS}
                    record['Track'] = track_name
                    record['Race'] = current_race
                    record['Box'] = box
                    record['DogName'] = dog_name
                    record['SourcePDF'] = os.path.basename(pdf_path)
                    
                    # Extract timing and distance from this line and surrounding context
                    context_start = max(0, lines.index(line) - 2)
                    context_end = min(len(lines), lines.index(line) + 3)
                    context_text = '\n'.join(lines[context_start:context_end])
                    
                    record['Distance'] = extract_distance_from_text(context_text)
                    record['BestTime'] = extract_race_time_from_text(context_text)
                    
                    sect1, sect2, sect3 = extract_sectionals_from_text(context_text)
                    record['Sectional1'] = sect1
                    record['Sectional2'] = sect2
                    record['Sectional3'] = sect3
                    
                    # Calculate SpeedIndex if we have distance and time
                    if record['Distance'] and record['BestTime']:
                        try:
                            record['SpeedIndex'] = record['Distance'] / record['BestTime']
                        except (TypeError, ZeroDivisionError):
                            pass
                    
                    records.append(record)
                    logger.debug(f"  Added: Race={current_race}, Box={box}, Dog={dog_name}")
    
    except Exception as e:
        logger.error(f"Error parsing {pdf_path}: {e}")
    
    logger.info(f"Extracted {len(records)} records from {pdf_path}")
    return records


def parse_directory(data_dir: str) -> pd.DataFrame:
    """Parse all PDFs in directory with per-file race context."""
    all_records = []
    
    pdf_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    logger.info(f"Found {len(pdf_files)} PDF files")
    
    for pdf_path in sorted(pdf_files):
        records = parse_pdf_with_race_context(pdf_path)
        all_records.extend(records)
    
    if not all_records:
        logger.warning("No records extracted")
        return pd.DataFrame(columns=COLUMNS)
    
    df = pd.DataFrame(all_records, columns=COLUMNS)
    
    # Enforce dtypes
    df['Track'] = df['Track'].astype(str)
    df['Race'] = pd.to_numeric(df['Race'], errors='coerce').astype('Int64')
    df['Box'] = pd.to_numeric(df['Box'], errors='coerce').astype('Int64')
    
    # Sort strictly: Track -> Race -> Box
    df = df.sort_values(['Track', 'Race', 'Box']).reset_index(drop=True)
    
    logger.info(f"Total records after parsing: {len(df)}")
    return df


def validate_output(df: pd.DataFrame) -> Dict[str, any]:
    """Validate the parsed dataframe."""
    validation_results = {}
    
    # Race count validation
    validation_results['race_counts'] = {}
    for track in df['Track'].unique():
        track_df = df[df['Track'] == track]
        unique_races = sorted(track_df['Race'].dropna().unique())
        validation_results['race_counts'][track] = {
            'count': len(unique_races),
            'races': unique_races
        }
    
    # Coverage validation
    validation_results['coverage'] = {}
    for col in ['Distance', 'BestTime', 'Sectional1', 'Sectional2', 'Sectional3']:
        if col in df.columns:
            total = len(df)
            populated = df[col].notna().sum()
            pct = (populated / total * 100) if total > 0 else 0
            validation_results['coverage'][col] = {
                'populated': populated,
                'total': total,
                'percent': pct
            }
    
    # Uniqueness validation
    duplicates = df.duplicated(subset=['Track', 'Race', 'Box'], keep=False)
    validation_results['duplicates'] = {
        'count': duplicates.sum(),
        'rows': df[duplicates][['Track', 'Race', 'Box', 'DogName']].to_dict('records') if duplicates.any() else []
    }
    
    return validation_results

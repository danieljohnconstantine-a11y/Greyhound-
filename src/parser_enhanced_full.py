#!/usr/bin/env python3
"""
Enhanced greyhound race form parser - extracts all 62 fields.
Maps to exact column specification provided by user.
"""

import os
import re
import logging
from typing import List, Dict, Optional, Tuple
import pdfplumber
import pandas as pd
from pathlib import Path

# Import detailed extractor
from detailed_extractor import enrich_record_with_details

# Configure logging
logger = logging.getLogger(__name__)

# Regex patterns
RACE_HEADER_PATTERN = re.compile(r"Race\s+No?\s+(\d+)", re.IGNORECASE)
ALT_RACE_PATTERN = re.compile(r"(?:Broken\s+Hill|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+Race\s+(\d+)", re.IGNORECASE)
DISTANCE_PATTERN = re.compile(r"(\d+)m")

# Exact column order as specified
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


def extract_track_from_filename(filename: str) -> Optional[str]:
    """Extract track code from filename"""
    basename = os.path.basename(filename)
    match = re.match(r"^([A-Z]{3,5})_", basename)
    if match:
        return match.group(1)
    return None


def extract_track_from_content(text: str) -> Optional[str]:
    """Extract track name from PDF content"""
    track_mappings = {
        'broken hill': 'BRHG',
        'capalaba': 'CAPA',
        'darwin': 'DRWN',
        'mandurah': 'MAND',
        'lakeside': 'QLAG',
        'straight': 'QSTR',
    }
    text_lower = text.lower()
    for name, code in track_mappings.items():
        if name in text_lower:
            return code
    return None


def extract_race_number(text: str) -> Optional[int]:
    """Extract race number"""
    match = RACE_HEADER_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    match = ALT_RACE_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def extract_distance(text: str) -> Optional[int]:
    """Extract distance in meters"""
    match = DISTANCE_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def extract_race_date(text: str) -> Optional[str]:
    """Extract race date (e.g., '07 Sept 25')"""
    match = re.search(r'(\d{2}\s+\w+\s+\d{2,4})', text)
    if match:
        return match.group(1)
    return None


def extract_race_class(text: str) -> Optional[str]:
    """Extract race class/grade"""
    patterns = [
        r'(MAIDEN|Maiden)',
        r'(\d+(?:st|nd|rd|th)\s+Grade)',
        r'(Grade\s+\d+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_race_time(text: str) -> Optional[str]:
    """Extract race time (e.g., 'Race Time 0:30.41' or '0:30.41')"""
    match = re.search(r'Race Time\s+([\d:\.]+)', text, re.IGNORECASE)
    if match:
        return match.group(1)
    # Try to find just the time format
    match = re.search(r'(\d+:\d+\.\d+)', text)
    if match:
        return match.group(1)
    return None


def extract_sectional_time(text: str) -> Optional[str]:
    """Extract sectional time (e.g., 'Sec Time 4.39' or '4.39')"""
    match = re.search(r'Sec Time\s+([\d\.]+)', text, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def extract_split_times(text: str) -> tuple:
    """Extract split/sectional times from race history"""
    # Try to find multiple sectional times
    sectionals = re.findall(r'Sec(?:tional)?\s*(?:Time)?\s*([\d\.]+)', text, re.IGNORECASE)
    
    sectional1 = sectionals[0] if len(sectionals) > 0 else None
    sectional2 = sectionals[1] if len(sectionals) > 1 else None
    sectional3 = sectionals[2] if len(sectionals) > 2 else None
    
    return sectional1, sectional2, sectional3


def parse_dog_summary_line(line: str, track: str, race: int, distance: Optional[int], 
                           race_date: Optional[str], race_class: Optional[str],
                           source_pdf: str) -> Optional[Dict]:
    """
    Parse the summary line for a dog (e.g., "1. 23534Big Eddie 2b 0.0kg 4 Claude Dacey...")
    Extracts basic fields from the compact summary format.
    """
    # Pattern for standard format
    box_match = re.match(r'\s*(\d+)\.\s+([\dx]{0,5})([A-Z][A-Za-z\'\s\-]+?)(?:\s+(\d+)([bdk]))', line)
    if not box_match:
        return None
    
    box_num = int(box_match.group(1))
    if box_num < 1 or box_num > 10:
        return None
    
    form_numbers = box_match.group(2) if box_match.group(2) else ""
    dog_name = box_match.group(3).strip()
    age = box_match.group(4)
    sex = box_match.group(5)
    
    # Extract rest of line components
    rest = line[box_match.end():]
    
    # Weight
    weight = None
    weight_match = re.search(r'(\d+\.\d+)kg', rest)
    if weight_match:
        weight = weight_match.group(1)
    
    # Trainer
    trainer = None
    trainer_match = re.search(r'kg\s+\d+\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', rest)
    if trainer_match:
        trainer = trainer_match.group(1).strip()
    
    # Career record (Wins - Places - Shows)
    wins, seconds, thirds = None, None, None
    career_match = re.search(r'(\d+)\s*-\s*(\d+)\s*-\s*(\d+)', rest)
    if career_match:
        wins = int(career_match.group(1))
        seconds = int(career_match.group(2))
        thirds = int(career_match.group(3))
    
    # Career prize money
    career_prize = None
    prize_match = re.search(r'\$([0-9,]+)', rest)
    if prize_match:
        career_prize = prize_match.group(1)
    
    # RTC, DLR, DLW (last 3 values before Mdn or end)
    rtc_match = re.search(r'\$[0-9,]+\s+([A-Z0-9]+)\s+(\d+)\s+(\d+|Mdn)', rest)
    track_condition, last_start_margin, margin = None, None, None
    if rtc_match:
        track_condition = rtc_match.group(1)
        last_start_margin = rtc_match.group(2)
        margin = rtc_match.group(3)
    
    # Calculate stats
    starts = (wins or 0) + (seconds or 0) + (thirds or 0)
    win_rate = f"{(wins/starts*100):.1f}%" if starts > 0 and wins else "0%"
    place_rate = f"{((wins+seconds)/starts*100):.1f}%" if starts > 0 else "0%"
    
    # Build record dictionary with all 62 columns
    record = {
        "Track": track,
        "Race": race,
        "Box": box_num,
        "DogName": dog_name,
        "Trainer": trainer,
        "Grade": race_class,  # from race header
        "Distance": distance,
        "RaceDate": race_date,
        "Form": form_numbers,
        "WinRate": win_rate,
        "PlaceRate": place_rate,
        "Odds": None,  # Not in summary, would be in detailed section
        "BestTime": None,
        "Margin": margin,
        "Sectional1": None,
        "Sectional2": None,
        "Sectional3": None,
        "RaceComment": None,
        "Starts": starts if starts > 0 else None,
        "Wins": wins,
        "Seconds": seconds,
        "Thirds": thirds,
        "CareerPrizeMoney": career_prize,
        "CareerBest": None,
        "RaceClass": race_class,
        "TrackCondition": track_condition,
        "Weather": None,
        "Interference": None,
        "TrainerWinRate": None,
        "TrainerState": None,
        "TrainerCity": None,
        "Owner": None,  # In detailed section
        "Sire": None,  # In detailed section
        "Dam": None,  # In detailed section
        "Age": age,
        "Sex": sex,
        "Color": None,  # In detailed section
        "Weight": weight,
        "LastStartDate": None,
        "LastStartTrack": None,
        "LastStartResult": None,
        "LastStartMargin": last_start_margin,
        "LastStartComment": None,
        "SplitAvg": None,
        "SpeedIndex": None,
        "ConsistencyIndex": None,
        "TrackDistanceWins": None,
        "TrackDistancePlaces": None,
        "TrackStarts": None,
        "TrackWins": None,
        "DistanceStarts": None,
        "DistanceWins": None,
        "DistancePlaces": None,
        "BoxHistory": None,
        "BoxWins": None,
        "BoxPlaces": None,
        "EarlySpeed": None,
        "ClosingSpeed": None,
        "Score": None,
        "Comments": None,
        "Notes": None,
        "SourcePDF": source_pdf
    }
    
    return record


def parse_pdf_file(filepath: str) -> pd.DataFrame:
    """Parse a single PDF file and extract all fields"""
    logger.info(f"Parsing PDF: {filepath}")
    
    track = extract_track_from_filename(filepath)
    if not track:
        track = "UNKNOWN"
    
    source_pdf = os.path.basename(filepath)
    all_rows = []
    
    try:
        with pdfplumber.open(filepath) as pdf:
            logger.info(f"  Pages: {len(pdf.pages)}")
            
            current_race = None
            current_distance = None
            current_race_date = None
            current_race_class = None
            prev_race = None
            
            # Try to extract track from content if needed
            if track == "UNKNOWN" and len(pdf.pages) > 0:
                first_page_text = pdf.pages[0].extract_text()
                if first_page_text:
                    track_from_content = extract_track_from_content(first_page_text)
                    if track_from_content:
                        track = track_from_content
            
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if not text:
                    continue
                
                # Extract race-level information
                race_num = extract_race_number(text)
                if race_num and race_num != prev_race:
                    current_race = race_num
                    prev_race = race_num
                    logger.debug(f"  Found Race {race_num} on page {page_num}")
                
                distance = extract_distance(text)
                if distance:
                    current_distance = distance
                
                race_date = extract_race_date(text)
                if race_date:
                    current_race_date = race_date
                
                race_class = extract_race_class(text)
                if race_class:
                    current_race_class = race_class
                
                # Parse dog entries
                if current_race:
                    text_layout = page.extract_text(layout=True)
                    lines = text_layout.split('\n')
                    
                    for line in lines:
                        if not line.strip():
                            continue
                        
                        dog_record = parse_dog_summary_line(
                            line, track, current_race, current_distance,
                            current_race_date, current_race_class, source_pdf
                        )
                        if dog_record:
                            # Enrich with detailed section data
                            dog_record = enrich_record_with_details(dog_record, lines)
                            all_rows.append(dog_record)
    
    except Exception as e:
        logger.error(f"Error processing PDF {filepath}: {e}")
    
    # Create DataFrame with exact column order
    df = pd.DataFrame(all_rows, columns=COLUMNS)
    
    # Remove duplicates based on Track, Race, Box
    df = df.drop_duplicates(subset=['Track', 'Race', 'Box'], keep='first')
    
    # Sort by Race then Box to maintain proper ordering
    df = df.sort_values(['Race', 'Box']).reset_index(drop=True)
    
    logger.info(f"  Extracted {len(df)} dog records")
    
    return df


def parse_directory(directory: str) -> pd.DataFrame:
    """Parse all PDF files in directory"""
    logger.info(f"Parsing directory: {directory}")
    
    all_dfs = []
    pdf_files = []
    
    # Collect all PDFs recursively
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    pdf_files.sort()
    logger.info(f"Found {len(pdf_files)} PDF files")
    
    for filepath in pdf_files:
        try:
            df = parse_pdf_file(filepath)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {filepath}: {e}")
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # Final sort by Race and Box across all files
        combined_df = combined_df.sort_values(['Race', 'Box']).reset_index(drop=True)
        logger.info(f"Total records: {len(combined_df)}")
        return combined_df
    else:
        return pd.DataFrame(columns=COLUMNS)


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        df = parse_pdf_file(test_file)
        print("\n" + "="*80)
        print("SAMPLE OUTPUT (first 5 rows)")
        print("="*80)
        print(df.head(5).to_string())
        print(f"\nTotal records: {len(df)}")
        print(f"Columns: {len(df.columns)}")
    else:
        print("Usage: python parser_enhanced_full.py <pdf_file>")

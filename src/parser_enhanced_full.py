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

# Speed extraction regex patterns (QLAKG/NT format)
# QLAKG format: "0:20.00 Sec Time 1.87 BP 4..." (time comes first, no "Race Time" prefix)
RACE_TIME_RE = re.compile(r"(?:Race Time\s*)?(?P<time>\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2})\s+Sec Time")
SEC_TIME_RE = re.compile(r"Sec Time\s*(?P<sectional>\d{1,2}\.\d{2})")
SEC_TIME_ADJ_RE = re.compile(r"Sec Time Adj\s*(?P<adj>\d{1,2}\.\d{2})")
DISTANCE_RE = re.compile(r"Distance\s*(?P<dist>\d{2,3})m")

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
    # Try pattern with underscore: TRACK_date.pdf
    match = re.match(r"^([A-Z]{3,5})_", basename)
    if match:
        return match.group(1)
    # Try pattern without underscore: TRACKddmmform.pdf
    match = re.match(r"^([A-Z]{3,5})\d", basename)
    if match:
        return match.group(1)
    return None


def extract_track_from_content(text: str) -> Optional[str]:
    """Extract track name from PDF content"""
    track_mappings = {
        'broken hill': 'BRHG',
        'capalaba': 'CAPA',
        'darwin': 'DRWN',
        'mandurah': 'MANDG',
        'lakeside': 'QLAKG',
        'straight': 'QSTR',
        'angle park': 'SALEG',
        'wentworth park': 'WENTY',
        'the gardens': 'GAWL',
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


def extract_speed_variables(block_text: str) -> Dict:
    """
    Extract speed variables from a dog's text block (QLAKG/NT format).
    Returns: dict with Distance, BestTime, BestTimeSec, Sectional1, SectionalAdj,
             and computed metrics (SpeedIndex, SplitAvg, EarlySpeed, ClosingSpeed)
    """
    spd = {}
    
    # Extract Distance
    m_dist = DISTANCE_RE.search(block_text)
    if m_dist:
        spd["Distance"] = int(m_dist.group("dist"))
    
    # Extract Race Time (BestTime)
    m_race = RACE_TIME_RE.search(block_text)
    if m_race:
        raw_time = m_race.group("time")
        spd["BestTime"] = raw_time
        
        # Convert to seconds
        if ":" in raw_time:
            parts = raw_time.split(":")
            minutes = int(parts[0])
            seconds = float(parts[1])
            spd["BestTimeSec"] = (minutes * 60) + seconds
        else:
            spd["BestTimeSec"] = float(raw_time)
    
    # Extract Sectional Time (Sectional1)
    m_sec = SEC_TIME_RE.search(block_text)
    if m_sec:
        spd["Sectional1"] = float(m_sec.group("sectional"))
    
    # Extract Sectional Adjusted (optional)
    m_adj = SEC_TIME_ADJ_RE.search(block_text)
    if m_adj:
        spd["SectionalAdj"] = float(m_adj.group("adj"))
    
    # Compute derived metrics
    dist = spd.get("Distance")
    bt = spd.get("BestTimeSec")
    s1 = spd.get("Sectional1")
    
    # SplitAvg = Sectional1 (only one split for QLAKG format)
    if s1:
        spd["SplitAvg"] = s1
    
    # SpeedIndex = Distance / BestTimeSec (m/s)
    if dist and bt and bt > 0:
        spd["SpeedIndex"] = round(dist / bt, 2)
    
    # EarlySpeed = (Distance * 0.25) / Sectional1
    if dist and s1 and s1 > 0:
        spd["EarlySpeed"] = round((dist * 0.25) / s1, 2)
    
    # ClosingSpeed = (Distance * 0.75) / (BestTimeSec - Sectional1)
    if dist and bt and s1 and (bt - s1) > 0:
        spd["ClosingSpeed"] = round((dist * 0.75) / (bt - s1), 2)
    
    return spd


def enrich_record_with_detail_block(record: Dict, detail_block: str) -> Dict:
    """
    Enrich a dog record with data from the detail block (Section 2).
    Extracts race history and computes speed metrics.
    """
    if not detail_block:
        return record
    
    # Try to extract speed variables from the detail block using race history parser
    try:
        from race_history_parser import parse_race_history_line, extract_race_history_section
        
        # Extract race history lines from detail block
        history_lines = extract_race_history_section(detail_block)
        
        # Parse the most recent race (first line in history)
        if history_lines:
            most_recent = parse_race_history_line(history_lines[0])
            
            if most_recent:
                # Update record with extracted values
                if 'distance' in most_recent:
                    record['Distance'] = most_recent['distance']
                
                if 'race_time' in most_recent:
                    record['BestTime'] = most_recent['race_time']
                
                if 'sectional_time' in most_recent:
                    record['Sectional1'] = most_recent['sectional_time']
                
                # Compute derived metrics if we have the required data
                dist = record.get('Distance')
                bt_sec = most_recent.get('race_time_seconds')
                s1 = record.get('Sectional1')
                
                # SplitAvg = Sectional1 (only one split for QLAKG format)
                if s1:
                    record['SplitAvg'] = s1
                
                # SpeedIndex = Distance / BestTimeSec (m/s)
                if dist and bt_sec and bt_sec > 0:
                    record['SpeedIndex'] = round(dist / bt_sec, 2)
                
                # EarlySpeed = (Distance * 0.25) / Sectional1
                if dist and s1 and s1 > 0:
                    record['EarlySpeed'] = round((dist * 0.25) / s1, 2)
                
                # ClosingSpeed = (Distance * 0.75) / (BestTimeSec - Sectional1)
                if dist and bt_sec and s1 and (bt_sec - s1) > 0:
                    record['ClosingSpeed'] = round((dist * 0.75) / (bt_sec - s1), 2)
                
                # Extract finish position and margin
                if 'finish_position' in most_recent:
                    record['LastStartResult'] = f"{most_recent['finish_position']}"
                
                if 'margin' in most_recent:
                    record['LastStartMargin'] = str(most_recent['margin'])
    
    except Exception as e:
        # If race history parsing fails, try the old speed extraction method
        speed_data = extract_speed_variables(detail_block)
        if speed_data:
            # Merge speed data into record
            for key, value in speed_data.items():
                if value is not None:
                    record[key] = value
    
    # Extract additional fields from detail block
    # Owner
    owner_match = re.search(r'Owner[:\s]+([A-Z][A-Za-z\s\-]+?)(?:\s+Sire|$)', detail_block, re.IGNORECASE)
    if owner_match:
        record['Owner'] = owner_match.group(1).strip()
    
    # Sire and Dam
    sire_match = re.search(r'Sire[:\s]+([A-Z][A-Za-z\s\-]+?)(?:\s+Dam|$)', detail_block, re.IGNORECASE)
    if sire_match:
        record['Sire'] = sire_match.group(1).strip()
    
    dam_match = re.search(r'Dam[:\s]+([A-Z][A-Za-z\s\-]+?)(?:\s|$)', detail_block, re.IGNORECASE)
    if dam_match:
        record['Dam'] = dam_match.group(1).strip()
    
    # Color
    color_match = re.search(r'(bk|w|bd|f|bkw|be)\s+(dog|bitch)', detail_block.lower())
    if color_match:
        color_map = {'bk': 'Black', 'w': 'White', 'bd': 'Brindle', 'f': 'Fawn', 'bkw': 'Black & White', 'be': 'Blue'}
        record['Color'] = color_map.get(color_match.group(1), color_match.group(1))
    
    return record


def parse_dog_summary_line(line: str, track: str, race: int, distance: Optional[int], 
                           race_date: Optional[str], race_class: Optional[str],
                           source_pdf: str) -> Optional[Dict]:
    """
    Parse the summary line for a dog (e.g., "1. 23534Big Eddie 2b 0.0kg 4 Claude Dacey...")
    Extracts basic fields from the compact summary format.
    """
    # Pattern for standard format
    # Try multiple box patterns
    box_match = re.match(r'\s*(\d+)\.\s+([\dx]{0,5})([A-Z][A-Za-z\'\s\-]+?)(?:\s+(\d+)([bdk]))', line)
    if not box_match:
        # Try alternative patterns: [1], (1), Box 1, #1
        box_match = re.match(r'\s*[\[\(]?(\d{1,2})[\]\)]?\s+([\dx]{0,5})([A-Z][A-Za-z\'\s\-]+?)(?:\s+(\d+)([bdk]))', line)
    if not box_match:
        # Try pattern with Box/No prefix
        box_match = re.match(r'\s*(?:Box|No\.?|#)\s*(\d{1,2})\s+([\dx]{0,5})([A-Z][A-Za-z\'\s\-]+?)(?:\s+(\d+)([bdk]))', line)
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


def compute_speed_metrics(record: Dict) -> Dict:
    """Compute speed-related metrics from extracted data"""
    try:
        # Extract distance and race time
        distance = record.get('Distance')
        race_time_str = record.get('RaceTime')
        
        if distance and race_time_str:
            # Parse race time (formats: "0:30.41", "30.41", "00:30.41")
            race_time_seconds = None
            if ':' in str(race_time_str):
                parts = str(race_time_str).split(':')
                if len(parts) == 2:
                    minutes = float(parts[0])
                    seconds = float(parts[1])
                    race_time_seconds = (minutes * 60) + seconds
            else:
                try:
                    race_time_seconds = float(race_time_str)
                except:
                    pass
            
            # Compute SpeedIndex = Distance / Time (m/s)
            if race_time_seconds and race_time_seconds > 0:
                record['SpeedIndex'] = round(distance / race_time_seconds, 2)
        
        # Extract sectional times
        sectional1 = record.get('Sectional1')
        sectional2 = record.get('Sectional2')
        sectional3 = record.get('Sectional3')
        
        # Compute SplitAvg from available sectionals
        sectionals = [s for s in [sectional1, sectional2, sectional3] if s]
        if sectionals:
            try:
                numeric_sectionals = [float(s) for s in sectionals]
                record['SplitAvg'] = round(sum(numeric_sectionals) / len(numeric_sectionals), 2)
                
                # EarlySpeed from first sectional (if distance available)
                if numeric_sectionals and distance:
                    record['EarlySpeed'] = round((distance * 0.25) / numeric_sectionals[0], 2) if numeric_sectionals[0] > 0 else None
                
                # ClosingSpeed from last sectional
                if len(numeric_sectionals) >= 2:
                    record['ClosingSpeed'] = round((distance * 0.25) / numeric_sectionals[-1], 2) if numeric_sectionals[-1] > 0 else None
            except:
                pass
                
    except Exception as e:
        logger.debug(f"Error computing speed metrics: {e}")
    
    return record


def parse_pdf_file(filepath: str) -> pd.DataFrame:
    """Parse a single PDF file and extract all fields from ALL races"""
    logger.info(f"Parsing PDF: {filepath}")
    
    track = extract_track_from_filename(filepath)
    if not track:
        track = "UNKNOWN"
    
    source_pdf = os.path.basename(filepath)
    all_rows = []
    
    try:
        with pdfplumber.open(filepath) as pdf:
            logger.info(f"  Pages: {len(pdf.pages)}")
            
            # Try to extract track from content if needed
            if track == "UNKNOWN" and len(pdf.pages) > 0:
                first_page_text = pdf.pages[0].extract_text()
                if first_page_text:
                    track_from_content = extract_track_from_content(first_page_text)
                    if track_from_content:
                        track = track_from_content
            
            # Extract all text from all pages
            full_text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    full_text += page_text + "\n"
            
            # Find ALL race headers in the document
            race_headers = re.findall(r'Race\s*(?:No\.?\s*)?(\d+)', full_text, re.IGNORECASE)
            unique_races = sorted(set(int(r) for r in race_headers if r.isdigit()))
            
            logger.info(f"  Found {len(unique_races)} unique races: {unique_races}")
            
            # Process each race separately
            for race_num in unique_races:
                race_rows = []
                current_distance = None
                current_race_date = None
                current_race_class = None
                
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    if not text:
                        continue
                    
                    # Check if this page contains this race
                    race_match = re.search(r'Race\s*(?:No\.?\s*)?' + str(race_num) + r'\b', text, re.IGNORECASE)
                    if not race_match:
                        continue
                    
                    logger.debug(f"  Processing Race {race_num} on page {page_num}")
                    
                    # Extract race-level information for this race
                    distance = extract_distance(text)
                    if distance:
                        current_distance = distance
                    
                    race_date = extract_race_date(text)
                    if race_date:
                        current_race_date = race_date
                    
                    race_class = extract_race_class(text)
                    if race_class:
                        current_race_class = race_class
                    
                    # Parse dog entries on this page for this race
                    text_layout = page.extract_text(layout=True)
                    lines = text_layout.split('\n')
                    
                    lines_checked = 0
                    lines_matched = 0
                    for line in lines:
                        if not line.strip():
                            continue
                        
                        # Check if line looks like a dog line for verbose logging
                        if re.match(r'^\s*\d+\.\s', line):
                            lines_checked += 1
                            logger.debug(f"    Checking dog line: {line[:80]}")
                        
                        dog_record = parse_dog_summary_line(
                            line, track, race_num, current_distance,
                            current_race_date, current_race_class, source_pdf
                        )
                        if dog_record:
                            lines_matched += 1
                            logger.debug(f"      ✓ Matched: Box {dog_record['Box']}, Dog: {dog_record['DogName']}")
                            # Enrich with detailed section data
                            dog_record = enrich_record_with_details(dog_record, lines)
                            # Compute speed metrics
                            dog_record = compute_speed_metrics(dog_record)
                            race_rows.append(dog_record)
                    
                    if lines_checked > 0:
                        logger.debug(f"    Page {page_num}: Checked {lines_checked} dog lines, matched {lines_matched}")
                
                # Add all dogs from this race
                all_rows.extend(race_rows)
                logger.debug(f"  Race {race_num}: extracted {len(race_rows)} dogs")
                if len(race_rows) == 0 and logger.level == logging.DEBUG:
                    logger.warning(f"  ⚠️  Race {race_num} detected but 0 dogs extracted - check regex patterns")
    
    except Exception as e:
        logger.error(f"Error processing PDF {filepath}: {e}")
    
    # Create DataFrame with exact column order
    df = pd.DataFrame(all_rows, columns=COLUMNS)
    
    # Remove duplicates based on Track, Race, Box
    df = df.drop_duplicates(subset=['Track', 'Race', 'Box'], keep='first')
    
    # Sort by Race then Box to maintain proper ordering
    df = df.sort_values(['Race', 'Box']).reset_index(drop=True)
    
    logger.info(f"  Extracted {len(df)} dog records from {len(unique_races) if 'unique_races' in locals() else 0} races")
    
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
        
        # Remove any duplicates from overlapping PDFs (Track, Race, Box combination)
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['Track', 'Race', 'Box'], keep='first')
        after_dedup = len(combined_df)
        if before_dedup > after_dedup:
            logger.info(f"  Removed {before_dedup - after_dedup} duplicate entries from overlapping PDFs")
        
        # Final sort by Track -> Race -> Box to ensure proper ordering for batch processing
        if 'Track' in combined_df.columns and 'Race' in combined_df.columns and 'Box' in combined_df.columns:
            combined_df = combined_df.sort_values(['Track', 'Race', 'Box']).reset_index(drop=True)
        else:
            combined_df = combined_df.sort_values(['Race', 'Box']).reset_index(drop=True)
        
        logger.info(f"Total records: {len(combined_df)}")
        logger.info(f"  Tracks: {combined_df['Track'].nunique() if 'Track' in combined_df.columns else 'N/A'}")
        logger.info(f"  Races: {combined_df['Race'].nunique() if 'Race' in combined_df.columns else 'N/A'}")
        
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

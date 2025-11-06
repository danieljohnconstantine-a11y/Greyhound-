#!/usr/bin/env python3
"""
Layout-aware parser for greyhound race form PDFs.
Extracts race and dog information from multi-column Racing & Sports PDFs.
"""

import os
import re
import logging
from typing import List, Dict, Optional
import pdfplumber
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Regex patterns for parsing
RACE_HEADER_PATTERN = re.compile(r"Race\s+No?\s+(\d+)", re.IGNORECASE)
# Alternative pattern for "Broken Hill Race 6" or "Track Name Race X"
ALT_RACE_PATTERN = re.compile(r"(?:Broken\s+Hill|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+Race\s+(\d+)", re.IGNORECASE)
DOG_NAME_PATTERN = re.compile(r"^([A-Z][A-Za-z'\s\-]+)$")
DISTANCE_PATTERN = re.compile(r"(\d+)m")
PRIZE_PATTERN = re.compile(r"\$([0-9,]+)")
TRACK_PATTERN = re.compile(r"[A-Z\s]+(?=\s+\d+m)")


def extract_track_from_filename(filename: str) -> Optional[str]:
    """Extract track code from filename (e.g., QSTR_2025-09-08.pdf -> QSTR)"""
    basename = os.path.basename(filename)
    match = re.match(r"^([A-Z]{4})_", basename)
    if match:
        return match.group(1)
    # Try other patterns like BRHG, CAPAG, DRWNG
    match = re.match(r"^([A-Z]{3,5})", basename)
    if match:
        return match.group(1)
    return None


def extract_track_from_content(text: str) -> Optional[str]:
    """Extract track name from PDF content (e.g., 'Broken Hill' -> 'BRHG')"""
    # Check for known track names
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
    """Extract race number from text"""
    # Try standard pattern first
    match = RACE_HEADER_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    
    # Try alternative pattern (e.g., "Broken Hill Race 6")
    match = ALT_RACE_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    
    return None


def extract_distance(text: str) -> Optional[int]:
    """Extract distance in meters from text"""
    match = DISTANCE_PATTERN.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def extract_prize_money(text: str) -> Optional[str]:
    """Extract prize money from text"""
    match = PRIZE_PATTERN.search(text)
    if match:
        return match.group(1)
    return None


def parse_dog_line(words: List[Dict], start_idx: int) -> Optional[Dict]:
    """
    Parse a dog entry from word list.
    Returns dict with box, dog_name, trainer, and other available fields.
    """
    if start_idx >= len(words):
        return None
    
    # Look for box number (1-10) at start of line
    word = words[start_idx]
    text = word['text'].strip()
    
    # Check if this is a box number
    if not text.isdigit() or int(text) > 10:
        return None
    
    box_num = int(text)
    dog_data = {
        'box': box_num,
        'dog_name': None,
        'trainer': None,
        'prize_money': None,
        'odds': None,
        'margins': None,
        'comment': None
    }
    
    # Look for dog name in subsequent words (typically capitalized)
    idx = start_idx + 1
    name_parts = []
    
    while idx < len(words) and idx < start_idx + 10:
        w = words[idx]['text'].strip()
        
        # Dog names are typically all caps or title case
        if w and (w[0].isupper() or w.isalpha()):
            # Stop if we hit common field indicators
            if w in ['kg', 'D', 'MARK', 'Horse:', 'Career', 'Tab']:
                break
            name_parts.append(w)
            idx += 1
        else:
            break
    
    # Join all name parts without arbitrary length limits
    # Most dog names are 2-3 words, but some may be longer
    if name_parts:
        dog_data['dog_name'] = ' '.join(name_parts)
    
    return dog_data if dog_data['dog_name'] else None


def parse_page_layout_aware(page, track: str, race_num: int, distance: Optional[int]) -> List[Dict]:
    """
    Parse a single page using layout-aware extraction.
    Returns list of dog records.
    """
    rows = []
    
    try:
        # Extract text with layout preservation
        text = page.extract_text(layout=True)
        if not text:
            return rows
        
        # Parse lines
        lines = text.split('\n')
        
        # Track which boxes we've already seen for this race to avoid duplicates
        seen_boxes = set()
        
        for line_idx, line in enumerate(lines):
            # Don't strip - we need to preserve leading spaces for proper matching
            if not line.strip():
                continue
            
            # Look for dog entry lines: should have format "X. NNNNDogName Nd 0.0kg N Trainer ..."
            # This pattern is more specific: box number, then form numbers (may include 'x')+dog name starting with letter
            # Form numbers are typically 4-5 characters (e.g., "13582", "8x846", "48x48")
            # followed immediately by a capital letter starting the dog's name
            box_match = re.match(r'\s*(\d+)\.\s+([\dx]{4,5}[A-Z])', line)
            
            # Alternative pattern for Broken Hill format: "DogName21.37" (name + time)
            # Look for a capitalized word followed by a time, and ensure trainer name follows
            alt_match = None
            if not box_match:
                # Pattern: Dog name (1-3 words) + time (XX.XX) + trainer name (capitalized)
                alt_match = re.match(r'\s*([A-Z][A-Za-z\']+(?:\s+[A-Z][a-z]+){0,2})(\d+\.\d{2})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+', line)
            
            if box_match:
                box_num = int(box_match.group(1))
                
                if box_num >= 1 and box_num <= 10 and box_num not in seen_boxes:
                    seen_boxes.add(box_num)
                    
                    # Get the rest of the line after the box number and period
                    rest_of_line = line[box_match.end(1)+1:].strip()
                    
                    # Extract dog name (first sequence of words after the form numbers)
                    # Pattern matches format: "13582Doongalla Pedro 2d" or "8x846Archie Trick 4d"
                    # Captures: Dog name starting with capital letter, may include apostrophes/hyphens/spaces
                    # Stops at: age/sex indicator (e.g., "2d", "3b") or weight (e.g., "0.0kg")
                    dog_name_match = re.match(r'^[\dx]+([A-Z][A-Za-z\'\s\-]+?)(?:\s+\d[a-z]|\s+\d\.\d)', rest_of_line)
                    if dog_name_match:
                        dog_name = dog_name_match.group(1).strip()
                        
                        # Look for trainer name (capitalized words after weight and box)
                        # Pattern looks for: "kg N" followed by trainer name
                        trainer = None
                        trainer_match = re.search(r'kg\s+\d+\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', rest_of_line)
                        if trainer_match:
                            trainer = trainer_match.group(1).strip()
                        
                        # Extract prize money if present (format: $XX,XXX)
                        prize_money = None
                        prize_match = re.search(r'\$([0-9,]+)', rest_of_line)
                        if prize_match:
                            prize_money = prize_match.group(1)
                        
                        # Extract career record (format: N - N - N)
                        margins = None
                        margins_match = re.search(r'(\d+\s*-\s*\d+\s*-\s*\d+)', rest_of_line)
                        if margins_match:
                            margins = margins_match.group(1)
                        
                        rows.append({
                            'Track': track,
                            'RaceNo': race_num,
                            'Distance': distance,
                            'Box': box_num,
                            'DogName': dog_name,
                            'Trainer': trainer,
                            'PrizeMoney': prize_money,
                            'Odds': None,
                            'Margins': margins,
                            'Comment': None,
                            'RaceTime': None
                        })
            
            # Handle alternative format (e.g., Broken Hill: "DogName21.37 TrainerName")
            elif alt_match:
                dog_name = alt_match.group(1).strip()
                race_time = alt_match.group(2)
                rest = alt_match.group(3).strip()
                
                # Extract trainer (first capitalized words)
                trainer = None
                trainer_match = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', rest)
                if trainer_match:
                    trainer = trainer_match.group(1)
                
                # Extract margins (format: XX: N-N-N)
                margins = None
                margins_match = re.search(r'(\d+):\s+(\d+-\d+-\d+)', rest)
                if margins_match:
                    margins = margins_match.group(2)
                
                # Assign sequential box number based on order
                box_num = len(rows) % 10 + 1
                
                rows.append({
                    'Track': track,
                    'RaceNo': race_num,
                    'Distance': distance,
                    'Box': box_num,
                    'DogName': dog_name,
                    'Trainer': trainer,
                    'PrizeMoney': None,
                    'Odds': None,
                    'Margins': margins,
                    'Comment': None,
                    'RaceTime': race_time
                })
        
    except Exception as e:
        logger.error(f"Error parsing page: {e}")
    
    return rows


def parse_pdf_file(filepath: str) -> pd.DataFrame:
    """
    Parse a single PDF file and return DataFrame of all dogs.
    
    Args:
        filepath: Path to PDF file
        
    Returns:
        DataFrame with columns: Track, RaceNo, Distance, Box, DogName, Trainer,
                               PrizeMoney, Odds, Margins, Comment, RaceTime
    """
    logger.info(f"Parsing PDF: {filepath}")
    
    # Extract track from filename
    track = extract_track_from_filename(filepath)
    if not track:
        logger.warning(f"Could not extract track from filename: {filepath}")
        track = "UNKNOWN"
    
    all_rows = []
    
    try:
        with pdfplumber.open(filepath) as pdf:
            logger.info(f"  Pages: {len(pdf.pages)}")
            
            current_race = None
            current_distance = None
            prev_race = None
            
            # Try to extract track from first page content if not from filename
            if track == "UNKNOWN" and len(pdf.pages) > 0:
                first_page_text = pdf.pages[0].extract_text()
                if first_page_text:
                    track_from_content = extract_track_from_content(first_page_text)
                    if track_from_content:
                        track = track_from_content
                        logger.info(f"  Extracted track '{track}' from PDF content")
            
            for page_num, page in enumerate(pdf.pages, 1):
                # Extract text to find race headers
                text = page.extract_text()
                if not text:
                    continue
                
                # Look for race number
                race_num = extract_race_number(text)
                if race_num:
                    # Only update if different from previous
                    if race_num != prev_race:
                        current_race = race_num
                        prev_race = race_num
                        logger.debug(f"  Found Race {race_num} on page {page_num}")
                
                # Look for distance
                distance = extract_distance(text)
                if distance:
                    current_distance = distance
                
                # Parse dogs on this page
                if current_race:
                    dogs = parse_page_layout_aware(page, track, current_race, current_distance)
                    all_rows.extend(dogs)
                    logger.debug(f"    Extracted {len(dogs)} dogs from page {page_num}")
    
    except Exception as e:
        logger.error(f"Error processing PDF {filepath}: {e}")
    
    # Create DataFrame
    df = pd.DataFrame(all_rows, columns=[
        'Track', 'RaceNo', 'Distance', 'Box', 'DogName', 'Trainer',
        'PrizeMoney', 'Odds', 'Margins', 'Comment', 'RaceTime'
    ])
    
    # Remove duplicates based on Track, RaceNo, and Box
    df = df.drop_duplicates(subset=['Track', 'RaceNo', 'Box'], keep='first')
    
    logger.info(f"  Extracted {len(df)} dog records from {filepath}")
    
    return df


def parse_directory(directory: str) -> pd.DataFrame:
    """
    Parse all PDF files in a directory.
    
    Args:
        directory: Path to directory containing PDF files
        
    Returns:
        Combined DataFrame of all dogs from all PDFs
    """
    logger.info(f"Parsing directory: {directory}")
    
    all_dfs = []
    pdf_files = [f for f in os.listdir(directory) if f.endswith('.pdf')]
    
    logger.info(f"Found {len(pdf_files)} PDF files")
    
    for filename in sorted(pdf_files):
        filepath = os.path.join(directory, filename)
        try:
            df = parse_pdf_file(filepath)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to parse {filename}: {e}")
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total records: {len(combined_df)}")
        return combined_df
    else:
        logger.warning("No data extracted from any PDF")
        return pd.DataFrame(columns=[
            'Track', 'RaceNo', 'Distance', 'Box', 'DogName', 'Trainer',
            'PrizeMoney', 'Odds', 'Margins', 'Comment', 'RaceTime'
        ])


if __name__ == "__main__":
    # Simple test
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        df = parse_pdf_file(test_file)
        print(df.head(20))
        print(f"\nTotal records: {len(df)}")
    else:
        print("Usage: python parser_step1_complete_layoutaware.py <pdf_file>")

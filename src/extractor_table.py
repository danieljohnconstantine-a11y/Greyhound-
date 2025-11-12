#!/usr/bin/env python3
"""
extractor_table.py
==================
Extract Group C data (Race History Detail) from greyhound race PDFs.

Extracts historical race performance data from text-based table sections
in the PDF, including race results, timing, odds, and track information.

Includes OCR fallback for scanned/image-based PDFs using Tesseract.
"""

from __future__ import annotations
import re
import logging
import platform
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd
from pypdf import PdfReader

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_text_ocr(pdf_path: str | Path) -> str:
    """
    Extract text from PDF using OCR (Tesseract) for scanned/image-based PDFs.
    
    This function converts each PDF page to an image and uses pytesseract to
    extract text via OCR.
    
    Args:
        pdf_path: Path to the PDF file
    
    Returns:
        Combined text from all pages
    """
    try:
        from pdf2image import convert_from_path
        import pytesseract
        
        # Set Tesseract binary path based on OS
        if platform.system() == 'Windows':
            pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        # On Linux/Mac, tesseract should be in PATH
        
        logger.info(f"Using OCR fallback for {Path(pdf_path).name}")
        
        # Convert PDF to images
        images = convert_from_path(str(pdf_path))
        
        # Extract text from each image
        all_text = []
        for i, image in enumerate(images, 1):
            page_text = pytesseract.image_to_string(image)
            all_text.append(page_text)
            logger.debug(f"OCR extracted {len(page_text)} characters from page {i}")
        
        combined_text = '\n\n'.join(all_text)
        logger.info(f"OCR extraction complete: {len(combined_text)} total characters")
        
        return combined_text
        
    except ImportError as e:
        logger.error(f"OCR dependencies not available: {e}")
        logger.error("Install with: pip install pytesseract pdf2image")
        logger.error("Also requires: tesseract-ocr and poppler-utils system packages")
        return ""
    except Exception as e:
        logger.error(f"OCR extraction failed: {e}")
        return ""


# Pattern for race history lines
# Example: "4th of 7 2/10/2025 Mandurah Margin 7.5 Lengths Distance 300m SOT G RST MDN..."
RACE_HISTORY_PATTERN = re.compile(
    r'(\d+)(?:st|nd|rd|th)\s+of\s+(\d+)\s+'  # Finish position
    r'(\d{1,2}/\d{1,2}/\d{4})\s+'  # Date
    r'([A-Za-z]+)\s+'  # Track
    r'Margin\s+([\d\.]+)\s+Lengths\s+'  # Margin
    r'Distance\s+(\d+)m\s+'  # Distance
    r'SOT\s+([A-Z])\s+'  # SOT
    r'RST\s+([A-Z]+)\s+'  # RST
    r'Race\s+([^P]+?)\s+'  # Race name
    r'Prize\s+\$\s*([\d,]+)\s+'  # Prize
    r'API\s+([\d\.]+)',  # API
    re.MULTILINE
)

# Secondary pattern for time, odds, etc.
TIME_ODDS_PATTERN = re.compile(
    r'Time\s+([\d:\.]+)\s+'
    r'Sec Time\s+([\d\.]+)\s+'
    r'Sec Time Adj\s+([\d\.]+)\s+'
    r'BP\s+(\d+)\s+'
    r'Odds\s+([\d\.]+)',
    re.MULTILINE
)

# Pattern for winner/placed dogs
PLACINGS_PATTERN = re.compile(
    r'Winner\s+([^(]+?)\s+\((\d+)\)\s+'
    r'Second\s+([^(]+?)\s+\((\d+)\)\s+'
    r'Third\s+([^(]+?)\s+\((\d+)\)',
    re.MULTILINE
)

# Pattern for settled position and track direction
SETTLED_TRACK_PATTERN = re.compile(
    r'Settled\s+(\d+)(?:st|nd|rd|th).*?'
    r'Track Direction\s*([A-Za-z\-]+)?',
    re.DOTALL
)


def extract_race_history_from_text(text: str, dog_name: str, tab_no: str) -> List[Dict]:
    """
    Extract race history entries for a specific dog from text.
    
    Args:
        text: PDF text content
        dog_name: Name of the dog to extract history for
        tab_no: Tab/Box number of the dog
    
    Returns:
        List of race history records
    """
    history_records = []
    
    # Find all race history lines
    for match in RACE_HISTORY_PATTERN.finditer(text):
        record = {
            'Dog_Name': dog_name,
            'Tab_No': tab_no,
            'Hist_Finish_Pos': match.group(1),
            'Hist_Date': match.group(3),
            'Hist_Track': match.group(4),
            'Hist_Margin_L': match.group(5),
            'Hist_Distance': match.group(6) + 'm',
            'Hist_SOT': match.group(7),
            'Hist_RST': match.group(8),
            'Hist_Prize_Won': '$' + match.group(10),
            'Hist_API': match.group(11),
        }
        
        # Look for time and odds data nearby
        context_start = max(0, match.start() - 50)
        context_end = min(len(text), match.end() + 500)
        context = text[context_start:context_end]
        
        time_match = TIME_ODDS_PATTERN.search(context)
        if time_match:
            record['Hist_Race_Time'] = time_match.group(1)
            record['Hist_Sec_Time'] = time_match.group(2)
            record['Hist_Sec_Time_Adj'] = time_match.group(3)
            record['Hist_BP'] = time_match.group(4)
            record['Hist_Odds'] = time_match.group(5)
            
            # Calculate speed if we have time and distance
            try:
                distance_m = float(match.group(6))
                time_parts = time_match.group(1).split(':')
                if len(time_parts) == 2:
                    total_seconds = float(time_parts[0]) * 60 + float(time_parts[1])
                else:
                    total_seconds = float(time_parts[0])
                
                if total_seconds > 0:
                    speed_kmh = (distance_m / 1000) / (total_seconds / 3600)
                    record['Hist_Speed_km/h'] = f"{speed_kmh:.2f}"
                else:
                    record['Hist_Speed_km/h'] = 'N/A'
            except:
                record['Hist_Speed_km/h'] = 'N/A'
        else:
            record['Hist_Race_Time'] = 'N/A'
            record['Hist_Sec_Time'] = 'N/A'
            record['Hist_Sec_Time_Adj'] = 'N/A'
            record['Hist_BP'] = 'N/A'
            record['Hist_Odds'] = 'N/A'
            record['Hist_Speed_km/h'] = 'N/A'
        
        # Look for placings
        placings_match = PLACINGS_PATTERN.search(context)
        if placings_match:
            record['Hist_Winner'] = placings_match.group(1).strip()
            record['Hist_2nd_Place'] = placings_match.group(3).strip()
            record['Hist_3rd_Place'] = placings_match.group(5).strip()
        else:
            record['Hist_Winner'] = 'N/A'
            record['Hist_2nd_Place'] = 'N/A'
            record['Hist_3rd_Place'] = 'N/A'
        
        # Look for settled position and track direction
        settled_match = SETTLED_TRACK_PATTERN.search(context)
        if settled_match:
            record['Hist_Settled_Turn'] = settled_match.group(1) + 'th'
            record['Hist_Track_Direction'] = settled_match.group(2) if settled_match.group(2) else 'N/A'
        else:
            record['Hist_Settled_Turn'] = 'N/A'
            record['Hist_Track_Direction'] = 'N/A'
        
        # Ongoing winners - would need more context
        record['Hist_Ongoing_Winners'] = 'N/A'
        
        history_records.append(record)
    
    return history_records


def extract_history_data(pdf_path: str | Path) -> pd.DataFrame:
    """
    Extract all Group C (Race History Detail) data from a PDF.
    
    Uses pypdf for text extraction with automatic OCR fallback for
    scanned or image-based PDFs.
    
    Args:
        pdf_path: Path to the PDF file
    
    Returns:
        DataFrame with Race History Detail columns
    """
    pdf_path = Path(pdf_path)
    logger.info(f"Extracting race history from {pdf_path.name}")
    
    try:
        reader = PdfReader(str(pdf_path))
        all_history = []
        use_ocr = False
        
        # First, check if we need OCR
        for page_num, page in enumerate(reader.pages, 1):
            text = page.extract_text()
            
            # Check if text extraction failed or returned minimal text
            if len(text.strip()) < 10:
                logger.warning(f"Page {page_num}: Minimal text detected ({len(text)} chars), triggering OCR fallback")
                use_ocr = True
                break
        
        # If OCR is needed, extract text using Tesseract
        if use_ocr:
            text = extract_text_ocr(pdf_path)
            if not text:
                logger.error(f"OCR fallback failed for {pdf_path.name}")
                # Return empty DataFrame
                return pd.DataFrame(columns=_get_history_columns())
            
            # Process the OCR text as a single document
            # Find dog names
            dog_names = re.findall(r'^\d+\.\s*\n([A-Z][A-Z\s]+)\s*\n', text, re.MULTILINE)
            
            # For each dog, extract their race history
            for dog_name in dog_names:
                dog_name = dog_name.strip()
                
                # Find tab number for this dog
                tab_match = re.search(rf'{re.escape(dog_name)}.*?\((\d+)\)', text, re.DOTALL)
                tab_no = tab_match.group(1) if tab_match else 'N/A'
                
                # Extract history records
                records = extract_race_history_from_text(text, dog_name, tab_no)
                all_history.extend(records)
                
                logger.debug(f"Extracted {len(records)} history records for {dog_name} (OCR)")
        else:
            # Normal pypdf extraction
            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                
                # Find dog names on this page
                dog_names = re.findall(r'^\d+\.\s*\n([A-Z][A-Z\s]+)\s*\n', text, re.MULTILINE)
                
                # For each dog, extract their race history
                for dog_name in dog_names:
                    dog_name = dog_name.strip()
                    
                    # Find tab number for this dog
                    tab_match = re.search(rf'{re.escape(dog_name)}.*?\((\d+)\)', text, re.DOTALL)
                    tab_no = tab_match.group(1) if tab_match else 'N/A'
                    
                    # Extract history records
                    records = extract_race_history_from_text(text, dog_name, tab_no)
                    all_history.extend(records)
                    
                    logger.debug(f"Page {page_num}: Extracted {len(records)} history records for {dog_name}")
        
        # Create DataFrame with proper column order
        df = pd.DataFrame(all_history, columns=_get_history_columns())
        logger.info(f"Extracted {len(df)} race history records from {pdf_path.name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error extracting race history from {pdf_path}: {e}")
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=_get_history_columns())


def _get_history_columns() -> List[str]:
    """Return the standard column list for Race History DataFrame."""
    return [
        'Dog_Name', 'Tab_No', 'Hist_Date', 'Hist_Track', 'Hist_Distance', 'Hist_Finish_Pos',
        'Hist_Margin_L', 'Hist_Race_Time', 'Hist_Sec_Time', 'Hist_Sec_Time_Adj',
        'Hist_Speed_km/h', 'Hist_SOT', 'Hist_RST', 'Hist_BP', 'Hist_Odds', 'Hist_API',
        'Hist_Prize_Won', 'Hist_Winner', 'Hist_2nd_Place', 'Hist_3rd_Place',
        'Hist_Settled_Turn', 'Hist_Ongoing_Winners', 'Hist_Track_Direction'
    ]


if __name__ == "__main__":
    # Test with sample PDF
    test_pdf = "data/rns/2025-10-07/MANDG0710form (1).pdf"
    if Path(test_pdf).exists():
        df = extract_history_data(test_pdf)
        print(f"\n✓ Extracted {len(df)} race history records")
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nSample data (first 5 rows):")
        print(df.head(5))
        print(f"\nDogs with history: {df['Dog_Name'].unique().tolist()}")
    else:
        print(f"Test PDF not found: {test_pdf}")

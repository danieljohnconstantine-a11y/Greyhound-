#!/usr/bin/env python3
"""
extractor_text.py
=================
Extract Group A and Group B data (Dog Summary) from greyhound race PDFs.

Uses pypdf to extract text and regex patterns to capture all dog summary
variables including race info, performance metrics, and breeding data.

Includes OCR fallback for scanned/image-based PDFs using Tesseract.
"""

from __future__ import annotations
import re
import logging
import platform
from pathlib import Path
from typing import Dict, List, Optional

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


# Regex patterns for extracting various fields
RACE_NO_PATTERN = re.compile(r'Race No\s*\n+\s*(\d+)', re.IGNORECASE)
DOG_NAME_PATTERN = re.compile(r'^\d+\.\s*$\n([A-Z][A-Z\s]+)\s*$', re.MULTILINE)
TAB_NO_PATTERN = re.compile(r'Tab\s+FF.*?\n\s*(\d+)\.\s+\d+', re.MULTILINE)
AGE_SEX_PATTERN = re.compile(r'\((\d+)\)\s+([a-z]+)\s+(\d+)\s+([BDG])', re.MULTILINE)
WEIGHT_PATTERN = re.compile(r'(\d+\.?\d*)kg\s+\(\d+\)', re.MULTILINE)
TRAINER_PATTERN = re.compile(r'([A-Z][A-Z\s]+)\s*\nRaced Distance:', re.MULTILINE)
SIRE_DAM_PATTERN = re.compile(r'([A-Z][A-Z\s\'\-]+)\s+\([A-Z]+\)\s+-\s+([A-Z][A-Z\s\'\-]+)\s+\([A-Z]+\)', re.MULTILINE)
OWNER_PATTERN = re.compile(r'Owner:\s*([A-Za-z][A-Za-z\s\.\-]+)', re.MULTILINE)
CAREER_WPS_PATTERN = re.compile(r'Horse:\s*(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', re.MULTILINE)
PRIZE_MONEY_PATTERN = re.compile(r'\$\s*(\d+,?\d*)\s+(\d+)\s+(\d+)\s+Mdn', re.MULTILINE)
RTC_PATTERN = re.compile(r'\$\s*\d+,?\d*\s+(\d+)\s+(\d+)\s+Mdn', re.MULTILINE)
DLR_PATTERN = re.compile(r'\$\s*\d+,?\d*\s+\d+\s+(\d+)\s+Mdn', re.MULTILINE)
CAR_PM_PATTERN = re.compile(r'CarPM/s\s+12mPM/s.*?\n\$(\d+)\s+\$(\d+)', re.MULTILINE)
API_PATTERN = re.compile(r'CarPM/s\s+12mPM/s\s+API.*?\n\$\d+\s+\$\d+\s+([\d\.]+)', re.MULTILINE)
RTC_KM_PATTERN = re.compile(r'API\s+RTC/km.*?\n[\d\.]+\s+([\d\.]+)', re.MULTILINE)
TRAINER_STATS_PATTERN = re.compile(r't50s\s+t350s\s*\n\s*(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', re.MULTILINE)
DISTANCE_WPS_PATTERN = re.compile(r'Dist\s+ClockW.*?\n(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', re.MULTILINE)
COURSE_WPS_PATTERN = re.compile(r'Crs\s+Dist.*?\n(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', re.MULTILINE)
FF_FORM_PATTERN = re.compile(r'Tab\s+FF.*?\n\d+\.\s+(\d+)', re.MULTILINE)


def extract_dog_summary_from_text(text: str, race_no: Optional[int] = None) -> List[Dict]:
    """
    Extract dog summary data from a single race's text.
    
    Args:
        text: Text content from PDF
        race_no: Race number (if known)
    
    Returns:
        List of dictionaries, one per dog with all Group A & B fields
    """
    dogs = []
    
    # Split text into sections per dog (look for pattern like "1.\nDOG NAME")
    dog_sections = re.split(r'\n\d+\.\s*\n(?=[A-Z][A-Z\s]+\s*\n)', text)
    
    for section in dog_sections:
        if len(section.strip()) < 50:  # Skip tiny sections
            continue
        
        dog_data = {}
        
        # Group A: Basic dog info
        dog_data['Race_No'] = race_no or 'N/A'
        
        # Dog name (first uppercase line after number)
        name_match = re.search(r'^([A-Z][A-Z\s]+)\s*$', section, re.MULTILINE)
        dog_data['Dog_Name'] = name_match.group(1).strip() if name_match else 'N/A'
        
        # Tab number (from box number in table)
        tab_match = re.search(r'(\d+)kg\s+\((\d+)\)', section)
        dog_data['Tab_No'] = tab_match.group(2) if tab_match else 'N/A'
        
        # FF Form (first number after Tab FF in table)
        ff_match = re.search(r'^\d+\.\s+(\d+)', section, re.MULTILINE)
        dog_data['FF_Form'] = ff_match.group(1) if ff_match else 'N/A'
        
        # BP (Box Position) - same as Tab_No
        dog_data['BP'] = dog_data['Tab_No']
        
        # A/S (Age/Sex) - e.g., "bl 3 B" -> "3B"
        as_match = re.search(r'([bd]l)\s+(\d+)\s+([BDG])', section, re.IGNORECASE)
        if as_match:
            dog_data['A/S'] = f"{as_match.group(2)}{as_match.group(3)}"
        else:
            dog_data['A/S'] = 'N/A'
        
        # WT (kg)
        wt_match = re.search(r'([\d\.]+)kg\s+\(\d+\)', section)
        dog_data['WT (kg)'] = wt_match.group(1) if wt_match else 'N/A'
        
        # Trainer
        trainer_match = re.search(r'([A-Z][A-Za-z\s]+)\s*\n(?:KOBLENZ|WEST ON|Raced Distance)', section)
        if trainer_match:
            dog_data['Trainer'] = trainer_match.group(1).strip()
        else:
            dog_data['Trainer'] = 'N/A'
        
        # Sire and Dam
        sire_dam_match = re.search(r'([A-Z][A-Z\s\'\-]+)\s+\([A-Z]+\)\s+-\s+([A-Z][A-Z\s\'\-]+)\s+\([A-Z]+\)', section)
        if sire_dam_match:
            dog_data['Sire'] = sire_dam_match.group(1).strip()
            dog_data['Dam'] = sire_dam_match.group(2).strip()
        else:
            dog_data['Sire'] = 'N/A'
            dog_data['Dam'] = 'N/A'
        
        # Owner
        owner_match = re.search(r'Owner:\s*([A-Za-z][A-Za-z\s\.\-]+)', section)
        dog_data['Owner'] = owner_match.group(1).strip() if owner_match else 'N/A'
        
        # Group B: Performance metrics
        
        # Career W-P-S
        career_match = re.search(r'Horse:\s*(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', section)
        if career_match:
            dog_data['Career_W-P-S'] = f"{career_match.group(1)}-{career_match.group(2)}-{career_match.group(3)}"
        else:
            dog_data['Career_W-P-S'] = 'N/A'
        
        # Prize Money
        prize_match = re.search(r'\$\s*(\d+,?\d*)', section)
        dog_data['Prize_Money'] = prize_match.group(1) if prize_match else 'N/A'
        
        # RTC, DLR, DLW
        rtc_match = re.search(r'\$\s*\d+,?\d*\s+(\d+)\s+(\d+)\s+Mdn', section)
        if rtc_match:
            dog_data['RTC'] = rtc_match.group(1)
            dog_data['DLR'] = rtc_match.group(2)
        else:
            dog_data['RTC'] = 'N/A'
            dog_data['DLR'] = 'N/A'
        
        # DLW (Days Last Win)
        dog_data['DLW'] = '0'  # Default, would need more context
        
        # CarPM/s (G1), 12mPM/s (G2)
        pm_match = re.search(r'CarPM/s\s+12mPM/s.*?\n\$(\d+)\s+\$(\d+)', section)
        if pm_match:
            dog_data['Car_PM/s (G1)'] = pm_match.group(1)
            dog_data['12m_PM/s (G2)'] = pm_match.group(2)
        else:
            dog_data['Car_PM/s (G1)'] = 'N/A'
            dog_data['12m_PM/s (G2)'] = 'N/A'
        
        # API (G3)
        api_match = re.search(r'API\s+RTC/km.*?\n([\d\.]+)', section)
        dog_data['API (G3)'] = api_match.group(1) if api_match else 'N/A'
        
        # RTC/km
        rtc_km_match = re.search(r'RTC/km.*?\n[\d\.]+\s+([\d\.]+/[\d\.]+)', section)
        dog_data['RTC/km'] = rtc_km_match.group(1) if rtc_km_match else 'N/A'
        
        # Trainer Win% and Place%
        trainer_stats = re.search(r't50s\s+t350s\s*\n\s*(\d+)-(\d+)-(\d+)\s+(\d+)%-(\d+)%', section)
        if trainer_stats:
            dog_data['Trainer_Win_%'] = trainer_stats.group(4)
            dog_data['Trainer_Place_%'] = trainer_stats.group(5)
        else:
            dog_data['Trainer_Win_%'] = 'N/A'
            dog_data['Trainer_Place_%'] = 'N/A'
        
        # Raced Distance W-P-S
        dog_data['Raced_Dist_W-P-S'] = dog_data['Career_W-P-S']  # Simplified
        
        # Course W-P-S
        crs_match = re.search(r'Crs\s+Dist.*?\n(\d+)-(\d+)-(\d+)', section)
        if crs_match:
            dog_data['Crs_W-P-S'] = f"{crs_match.group(1)}-{crs_match.group(2)}-{crs_match.group(3)}"
        else:
            dog_data['Crs_W-P-S'] = dog_data['Career_W-P-S']
        
        # Dist W-P-S
        dist_match = re.search(r'Dist\s+ClockW.*?\n(\d+)-(\d+)-(\d+)', section)
        if dist_match:
            dog_data['Dist_W-P-S'] = f"{dist_match.group(1)}-{dist_match.group(2)}-{dist_match.group(3)}"
        else:
            dog_data['Dist_W-P-S'] = dog_data['Career_W-P-S']
        
        # FU W-P-S (Fast/Up)
        fu_match = re.search(r'FU\s+2U.*?\n(\d+)-(\d+)-(\d+)', section)
        if fu_match:
            dog_data['FU_W-P-S'] = f"{fu_match.group(1)}-{fu_match.group(2)}-{fu_match.group(3)}"
        else:
            dog_data['FU_W-P-S'] = '0-0-0'
        
        # 2U W-P-S
        two_u_match = re.search(r'2U\s+3U.*?\n(\d+)-(\d+)-(\d+)', section)
        if two_u_match:
            dog_data['2U_W-P-S'] = f"{two_u_match.group(1)}-{two_u_match.group(2)}-{two_u_match.group(3)}"
        else:
            dog_data['2U_W-P-S'] = '0-0-0'
        
        # DOD (Date of Data) - extract from PDF or use current
        dod_match = re.search(r'(\d{2}\s+[A-Z][a-z]+\s+\d{2,4})', section)
        dog_data['DOD'] = dod_match.group(1) if dod_match else 'N/A'
        
        # Speed metrics - would need sectional time data
        dog_data['Avg_Speed_km/h'] = 'N/A'
        dog_data['Min_Speed_km/h'] = 'N/A'
        dog_data['Max_Speed_km/h'] = 'N/A'
        
        dogs.append(dog_data)
    
    return dogs


def extract_summary_data(pdf_path: str | Path) -> pd.DataFrame:
    """
    Extract all Group A and Group B data from a PDF.
    
    Uses pypdf for text extraction with automatic OCR fallback for
    scanned or image-based PDFs.
    
    Args:
        pdf_path: Path to the PDF file
    
    Returns:
        DataFrame with Dog Summary columns (one row per dog)
    """
    pdf_path = Path(pdf_path)
    logger.info(f"Extracting summary data from {pdf_path.name}")
    
    try:
        reader = PdfReader(str(pdf_path))
        all_dogs = []
        use_ocr = False
        
        for page_num, page in enumerate(reader.pages, 1):
            # Try pypdf text extraction first
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
                return pd.DataFrame(columns=_get_summary_columns())
            
            # Process the OCR text as a single document
            # Find all race numbers
            race_matches = list(RACE_NO_PATTERN.finditer(text))
            if not race_matches:
                # Extract with default race number
                dogs = extract_dog_summary_from_text(text, race_no=1)
                all_dogs.extend(dogs)
            else:
                # Process each race section
                for i, race_match in enumerate(race_matches):
                    race_no = int(race_match.group(1))
                    # Get text from this race to next race (or end)
                    start_pos = race_match.start()
                    end_pos = race_matches[i + 1].start() if i + 1 < len(race_matches) else len(text)
                    race_text = text[start_pos:end_pos]
                    
                    dogs = extract_dog_summary_from_text(race_text, race_no)
                    all_dogs.extend(dogs)
        else:
            # Normal pypdf extraction
            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                
                # Find race number on this page
                race_match = RACE_NO_PATTERN.search(text)
                race_no = int(race_match.group(1)) if race_match else page_num
                
                # Extract dogs from this page
                dogs = extract_dog_summary_from_text(text, race_no)
                all_dogs.extend(dogs)
                
                logger.debug(f"Page {page_num}: Extracted {len(dogs)} dogs for Race {race_no}")
        
        # Create DataFrame with proper column order
        df = pd.DataFrame(all_dogs, columns=_get_summary_columns())
        logger.info(f"Extracted {len(df)} dogs from {pdf_path.name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error extracting from {pdf_path}: {e}")
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=_get_summary_columns())


def _get_summary_columns() -> List[str]:
    """Return the standard column list for Dog Summary DataFrame."""
    return [
        'Race_No', 'Career_W-P-S', 'Avg_Speed_km/h', 'Dog_Name', 'Prize_Money', 'Min_Speed_km/h',
        'Tab_No', 'RTC', 'Max_Speed_km/h', 'FF_Form', 'DLR', 'BP', 'DLW', 'A/S', 'Car_PM/s (G1)',
        'WT (kg)', '12m_PM/s (G2)', 'Trainer', 'API (G3)', 'Sire', 'RTC/km', 'Dam', 'Trainer_Win_%',
        'Owner', 'Trainer_Place_%', 'Raced_Dist_W-P-S', 'Crs_W-P-S', 'Dist_W-P-S', 'FU_W-P-S',
        '2U_W-P-S', 'DOD'
    ]


if __name__ == "__main__":
    # Test with sample PDF
    import sys
    
    test_pdf = "data/rns/2025-10-07/MANDG0710form (1).pdf"
    if Path(test_pdf).exists():
        df = extract_summary_data(test_pdf)
        print(f"\n✓ Extracted {len(df)} dogs")
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nSample data (first 3 rows):")
        print(df.head(3))
        print(f"\nDog names: {df['Dog_Name'].tolist()[:5]}")
    else:
        print(f"Test PDF not found: {test_pdf}")

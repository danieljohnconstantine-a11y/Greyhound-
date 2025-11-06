#!/usr/bin/env python3
"""
Detailed section extractor for greyhound forms.
Extracts Sire, Dam, Owner, Color, and performance metrics from detailed dog sections.
Also extracts speed metrics from race history.
"""

import re
from typing import Dict, Optional, Tuple, List

try:
    from .race_history_parser import extract_speed_metrics_from_history, extract_race_history_section
except ImportError:
    from race_history_parser import extract_speed_metrics_from_history, extract_race_history_section


def extract_detailed_dog_info(lines: List[str], dog_name: str, box_num: int = None) -> Dict:
    """
    Extract detailed information from the dog's detailed section.
    The detailed section appears after the summary line and contains:
    - Sire and Dam (breeding)
    - Owner
    - Color
    - Performance metrics
    - Track condition records
    """
    details = {
        'Owner': None,
        'Sire': None,
        'Dam': None,
        'Color': None,
        'TrainerWinRate': None,
        'RacedDistance': None,
        'WinningDistance': None,
        'CarPM_per_s': None,
        '12mPM_per_s': None,
        'API': None,
        'RTC_per_km': None,
        'RDistTC': None,
        'DLS': None,
        'DLW_stat': None,
        'DOD': None,
        'Car_Record': None,
        '12m_Record': None,
        'Crs_Record': None,
        'Dist_Record': None,
        'ClockW': None,
        'AClockW': None,
        'G1_Record': None,
        'G2_Record': None,
        'G3_Record': None,
        'LR_Record': None,
        'FU_Record': None,
        '2U_Record': None,
        '3U_Record': None,
        'Firm_Record': None,
        'Good_Record': None,
        'Soft_Record': None,
        'Heavy_Record': None,
        'AW_Record': None,
        'Turf_Record': None,
        'BestTime': None,
        'SplitAvg': None,
        'SpeedIndex': None,
        'EarlySpeed': None,
        'ClosingSpeed': None,
    }
    
    # Find the detailed section for this dog
    in_detail = False
    detail_section = []
    
    for line in lines:
        # Start capturing when we find the dog name header with splits
        if dog_name.upper() in line.upper() and ('j50s' in line or 'j350s' in line):
            in_detail = True
            detail_section = [line]
            continue
        
        if in_detail:
            detail_section.append(line)
            # Stop when we hit the next dog's section
            if len(detail_section) > 2 and re.match(r'[A-Z][A-Z\s]+j50s', line):
                break
            # Or stop after ~15 lines
            if len(detail_section) > 15:
                break
    
    if not detail_section:
        return details
    
    # Join the section for easier parsing
    section_text = '\n'.join(detail_section)
    
    # Extract Color from line like: "1. 0kg (1) bdl 1 D MARK SAAL..."
    color_match = re.search(r'\(\d+\)\s+([a-z]{2,4})\s+\d+\s+[DM]', section_text)
    if color_match:
        details['Color'] = color_match.group(1)
    
    # Extract Sire and Dam: "AUSSIE INFRARED (AUS) - BABS KRAMER (AUS)"
    sire_dam_match = re.search(r'([A-Z][A-Z\s]+)\s*\([A-Z]+\)\s*-\s*([A-Z][A-Z\s]+)\s*\([A-Z]+\)', section_text)
    if sire_dam_match:
        details['Sire'] = sire_dam_match.group(1).strip()
        details['Dam'] = sire_dam_match.group(2).strip()
    
    # Extract Owner: "Owner: Mark Saal"
    owner_match = re.search(r'Owner:\s+(.+?)(?:\n|$)', section_text)
    if owner_match:
        details['Owner'] = owner_match.group(1).strip()
    
    # Extract J/T percentages: "J/T: 14%-42%"
    jt_match = re.search(r'J/T:\s+(\d+%-\d+%)', section_text)
    if jt_match:
        details['TrainerWinRate'] = jt_match.group(1)
    
    # Extract Raced Distance and Winning Distance
    dist_match = re.search(r'Raced Distance:\s+([0-9\-]+)', section_text)
    if dist_match:
        details['RacedDistance'] = dist_match.group(1)
    
    win_dist_match = re.search(r'Winning Distance:\s+(.+?)(?:\n|$|\s{2,})', section_text)
    if win_dist_match:
        details['WinningDistance'] = win_dist_match.group(1).strip()
    
    # Extract performance metrics from the stats line
    # Pattern: "CarPM/s 12mPM/s API RTC/km RDistTC DLS DLW DOD Car 12m Crs Dist ClockW AClockW"
    # Next line: "$0 $0 0.0 FU/0 0 0 0 FU - - - - - -"
    if 'CarPM/s' in section_text:
        lines_list = section_text.split('\n')
        for i, line in enumerate(lines_list):
            if 'CarPM/s' in line and i + 1 < len(lines_list):
                values_line = lines_list[i + 1].strip()
                parts = values_line.split()
                
                if len(parts) >= 14:
                    # Parse performance metrics (first 8 values)
                    details['CarPM_per_s'] = parts[0] if parts[0] not in ['-', ''] else None
                    details['12mPM_per_s'] = parts[1] if parts[1] not in ['-', ''] else None
                    details['API'] = parts[2] if parts[2] not in ['-', ''] else None
                    details['RTC_per_km'] = parts[3] if parts[3] not in ['-', ''] else None
                    details['RDistTC'] = parts[4] if parts[4] not in ['-', ''] else None
                    details['DLS'] = parts[5] if parts[5] not in ['-', ''] else None
                    details['DLW_stat'] = parts[6] if parts[6] not in ['-', ''] else None
                    details['DOD'] = parts[7] if parts[7] not in ['-', ''] else None
                    
                    # Parse career records (next 6 values)
                    details['Car_Record'] = parts[8] if parts[8] not in ['-', ''] else None
                    details['12m_Record'] = parts[9] if parts[9] not in ['-', ''] else None
                    details['Crs_Record'] = parts[10] if parts[10] not in ['-', ''] else None
                    details['Dist_Record'] = parts[11] if parts[11] not in ['-', ''] else None
                    details['ClockW'] = parts[12] if parts[12] not in ['-', ''] else None
                    details['AClockW'] = parts[13] if parts[13] not in ['-', ''] else None
                break
    
    # Extract track condition records
    # Pattern: "G1 G2 G3 LR FU 2U 3U Firm Good Soft Heavy AW Turf"
    # Next line: "- - - - - - - - - - - - -" or actual values
    if 'G1' in section_text and 'G2' in section_text:
        lines_list = section_text.split('\n')
        for i, line in enumerate(lines_list):
            if 'G1' in line and 'G2' in line and 'G3' in line and i + 1 < len(lines_list):
                values_line = lines_list[i + 1].strip()
                parts = values_line.split()
                
                if len(parts) >= 13:
                    details['G1_Record'] = parts[0] if parts[0] not in ['-', ''] else None
                    details['G2_Record'] = parts[1] if parts[1] not in ['-', ''] else None
                    details['G3_Record'] = parts[2] if parts[2] not in ['-', ''] else None
                    details['LR_Record'] = parts[3] if parts[3] not in ['-', ''] else None
                    details['FU_Record'] = parts[4] if parts[4] not in ['-', ''] else None
                    details['2U_Record'] = parts[5] if parts[5] not in ['-', ''] else None
                    details['3U_Record'] = parts[6] if parts[6] not in ['-', ''] else None
                    details['Firm_Record'] = parts[7] if parts[7] not in ['-', ''] else None
                    details['Good_Record'] = parts[8] if parts[8] not in ['-', ''] else None
                    details['Soft_Record'] = parts[9] if parts[9] not in ['-', ''] else None
                    details['Heavy_Record'] = parts[10] if parts[10] not in ['-', ''] else None
                    details['AW_Record'] = parts[11] if parts[11] not in ['-', ''] else None
                    details['Turf_Record'] = parts[12] if parts[12] not in ['-', ''] else None
                break
    
    # Extract speed metrics from race history
    # Find all race history lines in the document
    if box_num is not None:
        race_history_text = extract_race_history_section(lines, dog_name, box_num)
        if race_history_text:
            speed_metrics = extract_speed_metrics_from_history(race_history_text)
            # Merge speed metrics into details
            details['BestTime'] = speed_metrics.get('BestTime')
            details['SplitAvg'] = speed_metrics.get('SplitAvg')
            details['SpeedIndex'] = speed_metrics.get('SpeedIndex')
            details['EarlySpeed'] = speed_metrics.get('EarlySpeed')
            details['ClosingSpeed'] = speed_metrics.get('ClosingSpeed')
    
    return details


def enrich_record_with_details(record: Dict, lines: List[str]) -> Dict:
    """
    Enrich a basic dog record with detailed information.
    Also attempts QLAKG-style speed variable extraction.
    """
    dog_name = record.get('DogName', '')
    box_num = record.get('Box')
    if not dog_name:
        return record
    
    # Extract detailed info (pass box number for race history extraction)
    details = extract_detailed_dog_info(lines, dog_name, box_num)
    
    # Merge details into record
    for key, value in details.items():
        if key in record and value is not None:
            record[key] = value
    
    return record

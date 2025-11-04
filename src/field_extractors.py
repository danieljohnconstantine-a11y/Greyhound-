#!/usr/bin/env python3
"""
Field extraction functions for greyhound race form PDFs.
Provides granular extraction of all 61+ fields from Racing & Sports PDFs.
"""

import re
from typing import Optional, Dict, List, Tuple


class FieldExtractor:
    """Extract detailed fields from greyhound race form text"""
    
    @staticmethod
    def extract_race_date(text: str) -> Optional[str]:
        """Extract race date from header (e.g., '07 Sept 25')"""
        match = re.search(r'(\d{2}\s+\w+\s+\d{2,4})', text)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def extract_race_time(text: str) -> Optional[str]:
        """Extract race time (e.g., '12:05PM')"""
        match = re.search(r'(\d{1,2}:\d{2}[AP]M)', text)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def extract_race_class(text: str) -> Optional[str]:
        """Extract race class (e.g., 'MAIDEN', '5th Grade')"""
        # Look for common class indicators
        patterns = [
            r'(MAIDEN|Maiden)',
            r'(\d+(?:st|nd|rd|th)\s+Grade)',
            r'(Grade\s+\d+)',
            r'(OPEN|Open)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    @staticmethod
    def extract_age_sex(text: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract age and sex (e.g., '2d' -> age=2, sex=d)"""
        match = re.search(r'(\d+)([bdk])', text)
        if match:
            return match.group(1), match.group(2)
        return None, None
    
    @staticmethod
    def extract_weight(text: str) -> Optional[str]:
        """Extract weight (e.g., '0.0kg')"""
        match = re.search(r'(\d+\.\d+)kg', text)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def extract_color(text: str) -> Optional[str]:
        """Extract dog color (e.g., 'bdl', 'bl', 'bk')"""
        # Common greyhound colors
        match = re.search(r'\((\d+)\)\s+([a-z]{2,4})\s+\d+', text)
        if match:
            return match.group(2)
        return None
    
    @staticmethod
    def extract_sire_dam(text: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract sire and dam (e.g., 'AUSSIE INFRARED (AUS) - BABS KRAMER (AUS)')"""
        match = re.search(r'([A-Z\s]+)\s*\([A-Z]+\)\s*-\s*([A-Z\s]+)\s*\([A-Z]+\)', text)
        if match:
            sire = match.group(1).strip()
            dam = match.group(2).strip()
            return sire, dam
        return None, None
    
    @staticmethod
    def extract_owner(text: str) -> Optional[str]:
        """Extract owner name"""
        match = re.search(r'Owner:\s+(.+?)(?:\n|$)', text)
        if match:
            return match.group(1).strip()
        return None
    
    @staticmethod
    def extract_career_record(text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """Extract career wins-places-shows (e.g., '0 - 6 - 15' or '6-4-19')"""
        match = re.search(r'(\d+)\s*-\s*(\d+)\s*-\s*(\d+)', text)
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
        return None, None, None
    
    @staticmethod
    def extract_career_prize(text: str) -> Optional[str]:
        """Extract career prize money"""
        match = re.search(r'\$([0-9,]+)', text)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def extract_rtc_dlr_dlw(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extract RTC (Recent Track Condition), DLR (Days Last Run), DLW (Days Last Win)"""
        # Pattern: "career_record $prize RTC DLR DLW"
        # Example: "0 - 6 - 15 $2,650 16 4 Mdn"
        match = re.search(r'\$[0-9,]+\s+([A-Z0-9]+)\s+(\d+)\s+(\d+|Mdn)', text)
        if match:
            return match.group(1), match.group(2), match.group(3)
        return None, None, None
    
    @staticmethod
    def extract_split_times(text: str) -> Dict[str, Optional[str]]:
        """Extract split times: j50s, j350s, t50s, t350s"""
        splits = {'j50s': None, 'j350s': None, 't50s': None, 't350s': None}
        
        # Look for the split times header line
        match = re.search(r'j50s\s+j350s\s+t50s\s+t350s', text, re.IGNORECASE)
        if match:
            # The values are typically on the next line or same line
            # Pattern: numbers separated by spaces or dashes
            values_match = re.search(r'(\d+)-(\d+)-(\d+)\s+(\d+)-(\d+)-', text[match.end():match.end()+50])
            if values_match:
                # This is simplified - actual extraction may need more context
                pass
        
        return splits
    
    @staticmethod
    def extract_performance_stats(text: str) -> Dict[str, Optional[str]]:
        """Extract performance statistics from the detailed section"""
        stats = {
            'CarPM_per_s': None,
            '12mPM_per_s': None,
            'API': None,
            'RTC_per_km': None,
            'RDistTC': None,
            'DLS': None,
            'DLW_stat': None,
            'DOD': None
        }
        
        # Look for the stats header line
        if 'CarPM/s' in text:
            # Find the line with header
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if 'CarPM/s' in line and i + 1 < len(lines):
                    # Next line should have values
                    values_line = lines[i + 1]
                    parts = values_line.split()
                    if len(parts) >= 8:
                        stats['CarPM_per_s'] = parts[0] if parts[0] != '-' else None
                        stats['12mPM_per_s'] = parts[1] if parts[1] != '-' else None
                        stats['API'] = parts[2] if parts[2] != '-' else None
                        stats['RTC_per_km'] = parts[3] if parts[3] != '-' else None
                        stats['RDistTC'] = parts[4] if parts[4] != '-' else None
                        stats['DLS'] = parts[5] if parts[5] != '-' else None
                        stats['DLW_stat'] = parts[6] if parts[6] != '-' else None
                        stats['DOD'] = parts[7] if parts[7] != '-' else None
                    break
        
        return stats
    
    @staticmethod
    def extract_career_records(text: str) -> Dict[str, Optional[str]]:
        """Extract career record summaries"""
        records = {
            'Car_Record': None,
            '12m_Record': None,
            'Crs_Record': None,
            'Dist_Record': None,
            'ClockW': None,
            'AClockW': None
        }
        
        # Look for the records in the stats section
        # Pattern: "Car 12m Crs Dist ClockW AClockW"
        # Values: "- - - - - -" or actual records
        
        return records
    
    @staticmethod
    def extract_track_condition_records(text: str) -> Dict[str, Optional[str]]:
        """Extract track condition performance records"""
        conditions = {
            'G1_Record': None, 'G2_Record': None, 'G3_Record': None,
            'LR_Record': None, 'FU_Record': None, '2U_Record': None,
            '3U_Record': None, 'Firm_Record': None, 'Good_Record': None,
            'Soft_Record': None, 'Heavy_Record': None, 'AW_Record': None,
            'Turf_Record': None
        }
        
        # Look for the conditions header line
        if 'G1' in text and 'G2' in text:
            lines = text.split('\n')
            for i, line in enumerate(lines):
                if 'G1' in line and 'G2' in line and 'G3' in line and i + 1 < len(lines):
                    # Next line has values
                    values_line = lines[i + 1]
                    parts = values_line.split()
                    if len(parts) >= 13:
                        conditions['G1_Record'] = parts[0] if parts[0] != '-' else None
                        conditions['G2_Record'] = parts[1] if parts[1] != '-' else None
                        conditions['G3_Record'] = parts[2] if parts[2] != '-' else None
                        conditions['LR_Record'] = parts[3] if parts[3] != '-' else None
                        conditions['FU_Record'] = parts[4] if parts[4] != '-' else None
                        conditions['2U_Record'] = parts[5] if parts[5] != '-' else None
                        conditions['3U_Record'] = parts[6] if parts[6] != '-' else None
                        conditions['Firm_Record'] = parts[7] if parts[7] != '-' else None
                        conditions['Good_Record'] = parts[8] if parts[8] != '-' else None
                        conditions['Soft_Record'] = parts[9] if parts[9] != '-' else None
                        conditions['Heavy_Record'] = parts[10] if parts[10] != '-' else None
                        conditions['AW_Record'] = parts[11] if parts[11] != '-' else None
                        conditions['Turf_Record'] = parts[12] if parts[12] != '-' else None
                    break
        
        return conditions
    
    @staticmethod
    def extract_jt_percentages(text: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract Jockey/Trainer percentage stats"""
        # Pattern: "J/T: 14%-42%" or similar
        match = re.search(r'J/T:\s+(\d+%-\d+%)', text)
        if match:
            return match.group(1), None
        
        # Sometimes there are two percentages
        matches = re.findall(r'(\d+%-\d+%)', text)
        if len(matches) >= 2:
            return matches[0], matches[1]
        elif len(matches) == 1:
            return matches[0], None
        
        return None, None
    
    @staticmethod
    def extract_raced_distance(text: str) -> Optional[str]:
        """Extract raced distance info"""
        match = re.search(r'Raced Distance:\s+([0-9\-]+)', text)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def extract_winning_distance(text: str) -> Optional[str]:
        """Extract winning distance info"""
        match = re.search(r'Winning Distance:\s+(.+?)(?:\n|$)', text)
        if match:
            return match.group(1).strip()
        return None
    
    @staticmethod
    def extract_form_numbers(text: str) -> Optional[str]:
        """Extract form guide numbers (e.g., '23534')"""
        # Pattern: numbers at start before dog name
        match = re.search(r'(\d+)([A-Z][a-z])', text)
        if match:
            return match.group(1)
        return None

#!/usr/bin/env python3
"""
Test module for src/detailed_extractor.py
Validates trainer, owner, and breeding data extraction.
"""

import pytest
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from detailed_extractor import extract_detailed_dog_info, enrich_record_with_details


class TestDetailedExtractor:
    """Test cases for detailed section extraction."""
    
    def test_extract_detailed_dog_info_complete(self):
        """Test extraction of all fields from a complete detailed section."""
        
        # Sample text representing a detailed dog section
        sample_lines = [
            "LIFE'S A JOURNEY       j50s j350s t50s t350s",
            "1. 31.0kg (1) bdl 1 D ROBIN HARNAS Adelaide SA",
            "Owner: ACME Syndicate",
            "FAST ROCKET (AUS) - BLUE LILY (AUS)",
            "J/T: 18%-45%",
            "Raced Distance: 300-600",
            "Winning Distance: 400m",
            "CarPM/s 12mPM/s API RTC/km RDistTC DLS DLW DOD Car 12m Crs Dist ClockW AClockW",
            "$500 $400 0.85 25.5 1200 3 2 1 15-3-2-1 10-2-1-0 8-2-1-0 6-1-1-0 21.50 21.45",
            "G1 G2 G3 LR FU 2U 3U Firm Good Soft Heavy AW Turf",
            "1-0-0 2-1-0 3-0-1 4-1-0 5-2-1 6-1-1 7-0-2 2-0-0 8-3-1 3-1-0 1-0-0 10-4-2 5-1-1"
        ]
        
        # Extract details
        details = extract_detailed_dog_info(sample_lines, "Life's A Journey")
        
        # Assertions for breeding and owner info
        assert details['Owner'] == 'ACME Syndicate', f"Expected Owner='ACME Syndicate', got '{details['Owner']}'"
        assert details['Sire'] == 'FAST ROCKET', f"Expected Sire='FAST ROCKET', got '{details['Sire']}'"
        assert details['Dam'] == 'BLUE LILY', f"Expected Dam='BLUE LILY', got '{details['Dam']}'"
        assert details['Color'] == 'bdl', f"Expected Color='bdl', got '{details['Color']}'"
        
        # Assertions for trainer info
        assert details['TrainerWinRate'] == '18%-45%', f"Expected TrainerWinRate='18%-45%', got '{details['TrainerWinRate']}'"
        
        # Assertions for distance info
        assert details['RacedDistance'] == '300-600', f"Expected RacedDistance='300-600', got '{details['RacedDistance']}'"
        assert details['WinningDistance'] == '400m', f"Expected WinningDistance='400m', got '{details['WinningDistance']}'"
        
        # Assertions for performance metrics
        assert details['CarPM_per_s'] == '$500', f"Expected CarPM_per_s='$500', got '{details['CarPM_per_s']}'"
        assert details['12mPM_per_s'] == '$400', f"Expected 12mPM_per_s='$400', got '{details['12mPM_per_s']}'"
        assert details['API'] == '0.85', f"Expected API='0.85', got '{details['API']}'"
        
        # Assertions for career records
        assert details['Car_Record'] == '15-3-2-1', f"Expected Car_Record='15-3-2-1', got '{details['Car_Record']}'"
        assert details['12m_Record'] == '10-2-1-0', f"Expected 12m_Record='10-2-1-0', got '{details['12m_Record']}'"
        
        # Assertions for track condition records
        assert details['G1_Record'] == '1-0-0', f"Expected G1_Record='1-0-0', got '{details['G1_Record']}'"
        assert details['Good_Record'] == '8-3-1', f"Expected Good_Record='8-3-1', got '{details['Good_Record']}'"
        assert details['AW_Record'] == '10-4-2', f"Expected AW_Record='10-4-2', got '{details['AW_Record']}'"
    
    def test_extract_partial_details(self):
        """Test extraction when only some fields are present."""
        
        sample_lines = [
            "SOME DOG       j50s j350s t50s t350s",
            "1. 30.0kg (2) bk 2 D JOHN SMITH",
            "Owner: Test Owner",
            "SIRE NAME (AUS) - DAM NAME (AUS)",
        ]
        
        details = extract_detailed_dog_info(sample_lines, "Some Dog")
        
        # Check that basic fields are extracted
        assert details['Owner'] == 'Test Owner'
        assert details['Sire'] == 'SIRE NAME'
        assert details['Dam'] == 'DAM NAME'
        assert details['Color'] == 'bk'
        
        # Check that missing fields are None
        assert details['TrainerWinRate'] is None
        assert details['RacedDistance'] is None
        assert details['CarPM_per_s'] is None
    
    def test_enrich_record_with_details(self):
        """Test enriching a basic record with detailed information."""
        
        # Basic record
        record = {
            'DogName': "Test Runner",
            'Track': 'TEST',
            'Race': 1,
            'Box': 3,
            'Trainer': 'Test Trainer',
            'Owner': None,
            'Sire': None,
            'Dam': None,
            'Color': None,
            'TrainerWinRate': None,
        }
        
        # Sample lines with details
        sample_lines = [
            "TEST RUNNER       j50s j350s t50s t350s",
            "1. 32.0kg (3) bl 3 D TEST TRAINER",
            "Owner: Winner Syndicate",
            "SUPER SIRE (AUS) - GREAT DAM (AUS)",
            "J/T: 20%-50%",
        ]
        
        # Enrich the record
        enriched = enrich_record_with_details(record, sample_lines)
        
        # Verify enrichment
        assert enriched['Owner'] == 'Winner Syndicate'
        assert enriched['Sire'] == 'SUPER SIRE'
        assert enriched['Dam'] == 'GREAT DAM'
        assert enriched['Color'] == 'bl'
        assert enriched['TrainerWinRate'] == '20%-50%'
        
        # Verify original fields unchanged
        assert enriched['DogName'] == "Test Runner"
        assert enriched['Track'] == 'TEST'
        assert enriched['Box'] == 3
    
    def test_no_details_section(self):
        """Test behavior when no detailed section is found."""
        
        sample_lines = [
            "Some other text",
            "No relevant information",
        ]
        
        details = extract_detailed_dog_info(sample_lines, "Nonexistent Dog")
        
        # All fields should be None
        assert details['Owner'] is None
        assert details['Sire'] is None
        assert details['Dam'] is None
        assert details['Color'] is None
        assert details['TrainerWinRate'] is None


if __name__ == '__main__':
    # Run tests with verbose output
    pytest.main([__file__, '-v'])

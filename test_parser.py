#!/usr/bin/env python3
"""
test_parser.py - Tests for the PDF parser

Basic test suite for parser functionality.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add parser to path
sys.path.insert(0, os.path.dirname(__file__))

from parser.pdf_parser import parse_pdf, parse_folder, FNAME_RE, RACE_HEADER, DOG_LINE


def test_filename_pattern():
    """Test filename regex pattern."""
    valid = [
        "RICH_2025-09-07.pdf",
        "SALE_2025-10-13.pdf",
        "ABCD_2024-01-01.pdf",
    ]
    invalid = [
        "RICH_2025-09-07",  # no .pdf
        "rich_2025-09-07.pdf",  # lowercase
        "RICHM_2025-09-07.pdf",  # 5 chars
        "RIC_2025-09-07.pdf",  # 3 chars
        "RICH-2025-09-07.pdf",  # wrong separator
    ]
    
    print("Testing filename pattern...")
    for fn in valid:
        assert FNAME_RE.match(fn), f"Should match: {fn}"
    for fn in invalid:
        assert not FNAME_RE.match(fn), f"Should not match: {fn}"
    print("  ✓ Filename pattern tests passed")


def test_race_header_pattern():
    """Test race header regex."""
    test_cases = [
        ("Race No. 5", "5"),
        ("Race 12", "12"),
        ("RACE NO 3", "3"),
        ("race 1", "1"),
    ]
    
    print("Testing race header pattern...")
    for text, expected_num in test_cases:
        match = RACE_HEADER.search(text)
        assert match, f"Should match: {text}"
        assert match.group(2) == expected_num, f"Expected race {expected_num}, got {match.group(2)}"
    print("  ✓ Race header tests passed")


def test_dog_line_pattern():
    """Test dog line regex."""
    valid = [
        "1. FAST DOG",
        "2. SPEEDY PUP",
        "8. DOG-NAME",
        "3. DOG'S NAME",
    ]
    invalid = [
        "9. TOO HIGH",  # box > 8
        "0. ZERO BOX",  # box < 1
        "FAST DOG",  # no number
    ]
    
    print("Testing dog line pattern...")
    for line in valid:
        match = DOG_LINE.match(line)
        assert match, f"Should match: {line}"
    for line in invalid:
        match = DOG_LINE.match(line)
        assert not match, f"Should not match: {line}"
    print("  ✓ Dog line tests passed")


def test_parse_folder_empty():
    """Test parsing empty folder."""
    print("Testing empty folder parse...")
    with tempfile.TemporaryDirectory() as tmpdir:
        df = parse_folder(tmpdir)
        assert df.empty, "Should return empty DataFrame"
        assert list(df.columns) == ["track", "date", "race", "box", "runner"]
    print("  ✓ Empty folder test passed")


def run_all_tests():
    """Run all tests."""
    print("\n" + "="*60)
    print("Running parser tests")
    print("="*60 + "\n")
    
    try:
        test_filename_pattern()
        test_race_header_pattern()
        test_dog_line_pattern()
        test_parse_folder_empty()
        
        print("\n" + "="*60)
        print("All tests passed! ✓")
        print("="*60)
        return 0
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())

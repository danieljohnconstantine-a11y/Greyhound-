#!/usr/bin/env python3
"""
Test suite for the Greyhound PDF ingestion system.

This module contains basic tests for the core functionality.
Run with: python -m pytest tests/
"""

import unittest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


class TestPDFParsing(unittest.TestCase):
    """Tests for PDF parsing functionality."""
    
    def test_import_parse_pdf(self):
        """Test that parse_pdf module can be imported."""
        try:
            import parse_pdf
            self.assertIsNotNone(parse_pdf)
        except ImportError as e:
            self.fail(f"Failed to import parse_pdf: {e}")
    
    def test_filename_regex(self):
        """Test filename pattern matching."""
        import parse_pdf
        import re
        
        # Test valid filenames
        valid_names = [
            "SALE_2025-09-01.pdf",
            "RICH_2024-12-31.pdf",
            "CAPA_2025-01-15.pdf"
        ]
        
        for name in valid_names:
            match = parse_pdf.FNAME_RE.match(name)
            self.assertIsNotNone(match, f"Should match valid filename: {name}")
            self.assertEqual(len(match.groups()), 2)
        
        # Test invalid filenames
        invalid_names = [
            "SALE_2025-09-01.txt",
            "sale_2025-09-01.pdf",
            "SALE2025-09-01.pdf",
            "SALE_20250901.pdf"
        ]
        
        for name in invalid_names:
            match = parse_pdf.FNAME_RE.match(name)
            self.assertIsNone(match, f"Should not match invalid filename: {name}")


class TestFetchForms(unittest.TestCase):
    """Tests for PDF fetching functionality."""
    
    def test_import_fetch_forms(self):
        """Test that fetch_forms module can be imported."""
        try:
            import fetch_forms
            self.assertIsNotNone(fetch_forms)
        except ImportError as e:
            self.fail(f"Failed to import fetch_forms: {e}")
    
    def test_reject_patterns(self):
        """Test that sponsor/promotional PDFs are rejected."""
        import fetch_forms
        
        # These should be rejected
        reject_cases = [
            ("ladbrokes_special.pdf", "context"),
            ("club_form.pdf", "context"),
            ("million_dollar_chase.pdf", "context"),
        ]
        
        for url, context in reject_cases:
            result = fetch_forms._looks_like_form(url, context)
            self.assertFalse(result, f"Should reject: {url}")


class TestMastercontrol(unittest.TestCase):
    """Tests for main automation script."""
    
    def test_import_mastercontrol(self):
        """Test that mastercontrol module can be imported."""
        try:
            import mastercontrol
            self.assertIsNotNone(mastercontrol)
        except ImportError as e:
            self.fail(f"Failed to import mastercontrol: {e}")
    
    def test_build_probabilities_empty(self):
        """Test probability calculation with empty DataFrame."""
        import pandas as pd
        import mastercontrol
        
        empty_df = pd.DataFrame()
        result = mastercontrol.build_probabilities(empty_df)
        
        self.assertTrue(result.empty)
        self.assertListEqual(
            list(result.columns),
            ["track", "date", "race", "box", "runner", "prob_win"]
        )
    
    def test_build_probabilities_basic(self):
        """Test probability calculation with sample data."""
        import pandas as pd
        import mastercontrol
        
        # Create sample data: 1 race with 4 runners
        sample_data = pd.DataFrame([
            {"track": "TEST", "date": "2025-01-01", "race": 1, "box": 1, "runner": "DOG A"},
            {"track": "TEST", "date": "2025-01-01", "race": 1, "box": 2, "runner": "DOG B"},
            {"track": "TEST", "date": "2025-01-01", "race": 1, "box": 3, "runner": "DOG C"},
            {"track": "TEST", "date": "2025-01-01", "race": 1, "box": 4, "runner": "DOG D"},
        ])
        
        result = mastercontrol.build_probabilities(sample_data)
        
        self.assertEqual(len(result), 4)
        self.assertTrue(all(result["prob_win"] == 0.25))
        self.assertIn("prob_win", result.columns)


class TestDataIntegrity(unittest.TestCase):
    """Tests for data validation and integrity."""
    
    def test_directory_structure(self):
        """Test that required directories exist."""
        root = Path(__file__).resolve().parent.parent
        
        required_dirs = [
            root / "src",
            root / "data" / "input",
            root / "data" / "output",
            root / "tests",
        ]
        
        for dir_path in required_dirs:
            self.assertTrue(
                dir_path.exists(),
                f"Required directory should exist: {dir_path}"
            )
    
    def test_required_files(self):
        """Test that required files exist."""
        root = Path(__file__).resolve().parent.parent
        
        required_files = [
            root / "requirements.txt",
            root / "README.md",
            root / "LICENSE",
            root / ".gitignore",
        ]
        
        for file_path in required_files:
            self.assertTrue(
                file_path.exists(),
                f"Required file should exist: {file_path}"
            )


if __name__ == "__main__":
    unittest.main()

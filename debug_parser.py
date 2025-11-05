#!/usr/bin/env python3
"""
debug_parser.py - Debug utility for PDF parser

This script helps debug the PDF parsing by showing:
- Raw text extraction from PDFs
- Pattern matching details
- Race and runner extraction process

Usage:
    python debug_parser.py <pdf_file>
    python debug_parser.py --folder <forms_directory>
"""

import sys
import argparse
from pathlib import Path
from parser.pdf_parser import parse_pdf, FNAME_RE, RACE_HEADER, DOG_LINE

try:
    from pdfminer.high_level import extract_text
except ImportError:
    print("Error: pdfminer.six not installed. Install with: pip install pdfminer.six")
    sys.exit(1)


def debug_single_pdf(pdf_path: str, show_raw: bool = False):
    """Debug a single PDF file."""
    print(f"\n{'='*60}")
    print(f"Debugging: {pdf_path}")
    print('='*60)
    
    path = Path(pdf_path)
    if not path.exists():
        print(f"Error: File not found: {pdf_path}")
        return
    
    # Check filename pattern
    filename = path.name
    print(f"\nFilename: {filename}")
    match = FNAME_RE.match(filename)
    if match:
        print(f"✓ Filename matches expected pattern")
        print(f"  Track: {match.group(1)}")
        print(f"  Date: {match.group(2)}")
    else:
        print(f"✗ Filename doesn't match pattern (expected: CODE_YYYY-MM-DD.pdf)")
        return
    
    # Extract raw text
    try:
        raw_text = extract_text(str(path))
        print(f"\n✓ Successfully extracted text ({len(raw_text)} chars)")
    except Exception as e:
        print(f"✗ Failed to extract text: {e}")
        return
    
    if show_raw:
        print("\nRaw text preview (first 1000 chars):")
        print("-" * 60)
        print(raw_text[:1000])
        print("-" * 60)
    
    # Parse and show results
    print("\nParsing results:")
    print("-" * 60)
    
    results = parse_pdf(str(path))
    
    if not results:
        print("✗ No data extracted")
        
        # Show why - analyze text line by line
        print("\nAnalyzing text line by line:")
        current_race = None
        for i, line in enumerate(raw_text.splitlines()[:50], 1):  # First 50 lines
            line = line.strip()
            if not line:
                continue
            
            # Check for race header
            rh = RACE_HEADER.search(line)
            if rh:
                current_race = rh.group(2)
                print(f"  Line {i}: [RACE {current_race}] {line[:60]}")
                continue
            
            # Check for dog line
            dm = DOG_LINE.match(line)
            if dm:
                print(f"  Line {i}: [DOG] Box {dm.group(1)} - {dm.group(2)[:30]}")
                continue
            
            # Show first few non-matching lines
            if i <= 20:
                print(f"  Line {i}: {line[:60]}")
    else:
        print(f"✓ Extracted {len(results)} runner entries")
        
        # Show by race
        current_race = None
        for entry in results:
            if entry['race'] != current_race:
                current_race = entry['race']
                print(f"\n  Race {current_race}:")
            print(f"    Box {entry['box']}: {entry['runner']}")


def debug_folder(folder_path: str):
    """Debug all PDFs in a folder."""
    path = Path(folder_path)
    if not path.exists():
        print(f"Error: Folder not found: {folder_path}")
        return
    
    pdf_files = sorted(path.glob("*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {folder_path}")
        return
    
    print(f"\nFound {len(pdf_files)} PDF files")
    print("="*60)
    
    total_entries = 0
    for pdf_file in pdf_files:
        results = parse_pdf(str(pdf_file))
        status = "✓" if results else "✗"
        print(f"{status} {pdf_file.name}: {len(results)} entries")
        total_entries += len(results)
    
    print("="*60)
    print(f"Total: {total_entries} runner entries from {len(pdf_files)} files")


def main():
    parser = argparse.ArgumentParser(
        description="Debug PDF parser for greyhound race forms"
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="PDF file or folder to debug"
    )
    parser.add_argument(
        "--folder",
        action="store_true",
        help="Treat path as folder and debug all PDFs"
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Show raw text extraction"
    )
    
    args = parser.parse_args()
    
    if not args.path:
        parser.print_help()
        return 1
    
    if args.folder:
        debug_folder(args.path)
    else:
        debug_single_pdf(args.path, args.raw)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

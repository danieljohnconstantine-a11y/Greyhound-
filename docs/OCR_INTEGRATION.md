# OCR Fallback Integration Guide

## Overview

The PDF extraction pipeline now includes automatic OCR (Optical Character Recognition) fallback for scanned or image-based PDFs that don't contain extractable text.

## How It Works

### Automatic Detection

The system automatically detects when OCR is needed:

1. **pypdf extraction** is attempted first for all PDFs
2. If a page contains **fewer than 10 characters** of extractable text, OCR fallback is triggered
3. The entire PDF is processed using Tesseract OCR
4. Extracted text is parsed using the same regex patterns

### Detection Logic

```python
# In extractor_text.py and extractor_table.py
for page in reader.pages:
    text = page.extract_text()
    
    if len(text.strip()) < 10:
        # Minimal text detected, trigger OCR
        use_ocr = True
        break
```

### OCR Function

```python
def extract_text_ocr(pdf_path: str | Path) -> str:
    """
    Extract text from PDF using Tesseract OCR.
    
    - Converts each PDF page to an image
    - Runs Tesseract on each image
    - Combines text from all pages
    """
```

## Installation

### Required Dependencies

Add to `requirements.txt`:
```
pytesseract==0.3.13
pdf2image==1.17.0
```

Install Python packages:
```bash
pip install pytesseract pdf2image
```

### System-Level Requirements

#### Windows
1. Download and install Tesseract OCR from:
   https://github.com/UB-Mannheim/tesseract/wiki
   
2. Install to: `C:\Program Files\Tesseract-OCR\`

3. The code automatically sets the path:
   ```python
   pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
   ```

#### Linux/Ubuntu
```bash
sudo apt-get update
sudo apt-get install tesseract-ocr poppler-utils
```

#### macOS
```bash
brew install tesseract poppler
```

## Usage

No code changes needed! The OCR fallback is automatic:

```python
from extractor_text import extract_summary_data
from extractor_table import extract_history_data

# Works with both text-based and scanned PDFs
df_summary = extract_summary_data("race_form.pdf")
df_history = extract_history_data("race_form.pdf")
```

## Logging

The system logs OCR activity:

### When OCR is Triggered
```
WARNING:extractor_text:Page 1: Minimal text detected (5 chars), triggering OCR fallback
INFO:extractor_text:Using OCR fallback for race_form.pdf
```

### OCR Progress
```
DEBUG:extractor_text:OCR extracted 1543 characters from page 1
DEBUG:extractor_text:OCR extracted 1621 characters from page 2
INFO:extractor_text:OCR extraction complete: 3164 total characters
```

### OCR Errors
```
ERROR:extractor_text:OCR dependencies not available: No module named 'pytesseract'
ERROR:extractor_text:Install with: pip install pytesseract pdf2image
ERROR:extractor_text:Also requires: tesseract-ocr and poppler-utils system packages
```

## Performance Considerations

### Speed Comparison

| Method | Speed per Page | Typical PDF (10 pages) |
|--------|----------------|------------------------|
| pypdf text extraction | ~0.1s | ~1s |
| Tesseract OCR | ~2-5s | ~20-50s |

**Recommendation**: OCR is only used when necessary (minimal extractable text).

## Testing

### Unit Tests

Run the OCR fallback test suite:
```bash
python tests/test_ocr_fallback.py
```

Tests verify:
1. ✓ OCR function exists and is callable
2. ✓ OCR triggers when text < 10 characters
3. ✓ Normal PDFs bypass OCR
4. ✓ Parsed DataFrames have correct columns
5. ✓ OCR text is parseable by existing regex patterns

## Summary

The OCR fallback provides robust handling of scanned PDFs while maintaining fast processing for normal text-based PDFs. The automatic detection ensures the system "just works" regardless of PDF type.

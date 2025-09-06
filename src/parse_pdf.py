import os
import re

def parse_pdf_text(text, debug_path=None, pdf_name="unknown.pdf"):
    """
    Parse greyhound runners from raw PDF text.
    Saves raw text to debug_path for inspection if provided.
    """

    # Save raw text for debugging
    if debug_path:
        os.makedirs(debug_path, exist_ok=True)
        debug_file = os.path.join(debug_path, f"{pdf_name}.txt")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(text)

    # Try to extract lines like "1. Runner Name", "Box 2 RunnerName", etc.
    runners = []
    for line in text.splitlines():
        line = line.strip()
        # Common race card patterns
        m = re.match(r"^(?:Box\s*)?(\d+)[\.\)]?\s+([A-Za-z' -]+)", line)
        if m:
            box = int(m.group(1))
            name = m.group(2).strip()
            runners.append({"box": box, "runner": name})

    return runners

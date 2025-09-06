import os
import re

def parse_form_pdf(text, debug_path=None, pdf_name="unknown.pdf"):
    """
    Extract greyhound runner details from raw PDF text.
    Saves raw text into debug/ for troubleshooting.
    """
    # Save raw text for debugging
    if debug_path:
        os.makedirs(debug_path, exist_ok=True)
        debug_file = os.path.join(debug_path, f"{pdf_name}.txt")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(text)

    runners = []
    for line in text.splitlines():
        line = line.strip()
        # Match "Box 1 Runner Name", "1. Runner Name", etc.
        m = re.match(r"^(?:Box\s*)?(\d+)[\.\)]?\s+([A-Za-z' -]+)", line)
        if m:
            box = int(m.group(1))
            name = m.group(2).strip()
            runners.append({"box": box, "runner": name})

    return runners

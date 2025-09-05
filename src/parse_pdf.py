from __future__ import annotations
from pathlib import Path
import re
import pdfplumber
import pandas as pd
from .utils import RACE_RE, BOX_RE, DIST_RE, infer_track_and_date_from_name

COLUMNS = ["track", "date", "race", "distance_m", "box", "runner", "trainer", "comments", "form_text"]

def parse_form_pdf(pdf_path: Path) -> pd.DataFrame:
    """Very simple parser: read text lines and extract race, distance, box, runner."""
    track, dt = infer_track_and_date_from_name(pdf_path)
    rows = []
    race = None
    dist_m = None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            # update race and distance context if we see them
            r = RACE_RE.search(text)
            if r:
                race = int(r.group(1))
            d = DIST_RE.search(text)
            if d:
                dist_m = int(d.group(1))
            # scan lines for boxes/runners
            for line in text.splitlines():
                mbox = BOX_RE.search(line.strip())
                if not mbox:
                    continue
                box = int(mbox.group(1))
                # runner name: take the text after the box number
                runner = re.sub(r"^.*?(?:Box\\s*)?\\d\\s*[:\\-]?\\s*", "", line, flags=re.I).strip()
                # clean obvious times/numbers
                runner = re.sub(r"\\b(\\d{3,4}\\.\\d{2}|\\d{1,2}\\.\\d{2})\\b", "", runner).strip()
                if runner:
                    rows.append([track, dt, race, dist_m, box, runner, None, line.strip(), None])

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    out = pd.DataFrame(rows, columns=COLUMNS)
    out = out.dropna(subset=["box", "runner"], how="any")
    out["box"] = out["box"].astype(int)
    out.sort_values(["race", "box"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out

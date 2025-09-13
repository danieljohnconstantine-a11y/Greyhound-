import re, pathlib, csv
from PyPDF2 import PdfReader
from .utils import OUT_BASE, utcstamp

FORMS_DIR = pathlib.Path("forms")
OUT_DIR = OUT_BASE / "combined"

ROW = ["track", "date", "race", "box", "runner", "trainer"]

def _extract_text(pdf_path: pathlib.Path) -> str:
    txt = []
    try:
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            t = page.extract_text() or ""
            txt.append(t)
    except Exception as e:
        txt.append(f"__ERROR__ {e}")
    return "\n".join(txt)

def _parse_blocks(text: str, default_track: str):
    # Very loose heuristics: look for patterns "RACE 1", "Box 2  RunnerName (Trainer)"
    # Adjust as needed for your preferred PDF layout.
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    track = default_track
    date = ""
    race = None

    results = []

    # infer track/date from top lines if present
    for l in lines[:20]:
        m = re.search(r"(?:Track|Venue)[:\s]+([A-Za-z \-]+)", l, re.I)
        if m: track = m.group(1).strip().upper()
        d = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})", l)
        if d: date = d.group(1)

    for l in lines:
        r = re.search(r"\bRACE\s*(\d+)\b", l, re.I)
        if r:
            race = int(r.group(1))
            continue
        m = re.search(r"\b[Bb]ox\s*(\d+)\s+([A-Za-z' \-]+)\s*(?:\(([^)]+)\))?", l)
        if m and race is not None:
            box = int(m.group(1))
            runner = m.group(2).strip()
            trainer = (m.group(3) or "").strip()
            if runner and len(runner) >= 2:
                results.append([track, date, race, box, runner, trainer])
    return results

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = utcstamp()
    rows = []

    for pdf in sorted(FORMS_DIR.glob("*.pdf")):
        text = _extract_text(pdf)
        track_guess = pdf.stem.split("_")[0].upper()
        rows.extend(_parse_blocks(text, track_guess))

    out_csv = OUT_DIR / f"full_day_{ts}.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(ROW)
        for r in rows:
            w.writerow(r)

if __name__ == "__main__":
    main()

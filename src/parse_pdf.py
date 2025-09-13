# src/parse_pdf.py
# Usage: python src/parse_pdf.py --in forms --out data/combined/forms_<stamp>.csv
import argparse, csv, re
from pathlib import Path
import pdfplumber

HEADER = ["meeting","race","time","box","runner","odds","trainer","track","grade","source"]

def guess_meeting_from_name(fname: str) -> str:
    # e.g. DRWN_2025-09-07.pdf -> DRWN
    m = re.match(r"([A-Z]{3,5})[_-]", Path(fname).name)
    return (m.group(1) if m else Path(fname).stem).upper()

def normalize_time(s: str) -> str:
    s = s.strip()
    # accept 19:42, 7:42PM, 19.42, etc.
    s = s.replace(".", ":").upper()
    s = re.sub(r"\s+", "", s)
    # convert 7:42PM to 19:42
    m = re.match(r"^(\d{1,2}):(\d{2})(AM|PM)$", s)
    if m:
        h, mi, ap = int(m.group(1)), int(m.group(2)), m.group(3)
        if ap == "PM" and h < 12: h += 12
        if ap == "AM" and h == 12: h = 0
        return f"{h:02d}:{mi:02d}"
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else s

def is_runner_row(cells):
    # heuristic: Box number 1-8 + runner name present
    if len(cells) < 3: return False
    box = cells[0].strip()
    return box.isdigit() and 1 <= int(box) <= 8

def read_pdf(path: Path):
    meeting = guess_meeting_from_name(path.name)
    track = meeting  # you can map this to full track names if desired
    source = "rns_pdf"
    out_rows = []

    with pdfplumber.open(path) as pdf:
        race_no = None
        race_time = None
        grade = ""

        for page in pdf.pages:
            text = page.extract_text() or ""
            # Try to detect race header like "Race 5 – 19:42"
            for line in text.splitlines():
                m = re.search(r"Race\s+(\d+)\D+(\d{1,2}[:.]\d{2}(?:\s*[AP]M)?)", line, re.I)
                if m:
                    race_no = int(m.group(1))
                    race_time = normalize_time(m.group(2))
                g = re.search(r"(Grade\s+[A-Z0-9]+|Maiden|Open|Mixed\s?\d+)", line, re.I)
                if g: grade = g.group(1).strip()

            # Table extraction pass
            try:
                table = page.extract_table()
                tables = [table] if table else []
            except Exception:
                tables = []

            # Fallback: multiple tables
            try:
                tables = tables or page.extract_tables() or []
            except Exception:
                pass

            for tbl in tables:
                if not tbl: continue
                for row in tbl:
                    if not row: continue
                    cells = [ (c or "").strip() for c in row ]
                    if not is_runner_row(cells):
                        continue

                    box = cells[0].strip()
                    runner = cells[1].strip()
                    trainer = ""
                    odds = ""

                    # try to find trainer / odds in remaining columns
                    # common layout: [box, runner, trainer, odds, ...]
                    for c in cells[2:]:
                        if re.search(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b", c) and not trainer:
                            trainer = c.strip()
                        if re.match(r"^\d+(\.\d+)?$", c) and not odds:
                            odds = c.strip()

                    out_rows.append({
                        "meeting": meeting,
                        "race": race_no or "",
                        "time": race_time or "",
                        "box": box,
                        "runner": runner,
                        "odds": odds,
                        "trainer": trainer,
                        "track": track,
                        "grade": grade,
                        "source": source
                    })

    return out_rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", required=True)
    ap.add_argument("--out", dest="out_csv", required=True)
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(in_dir.glob("*.pdf"))
    rows = []
    for p in pdfs:
        try:
            rows.extend(read_pdf(p))
        except Exception as e:
            print(f"[WARN] Failed {p.name}: {e}")

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"parsed_rows={len(rows)} -> {out_csv}")

if __name__ == "__main__":
    main()

# src/parse_pdf.py
import re
import os
import glob
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List, Dict, Iterable
from pdfminer.high_level import extract_text

# ---- Data model -------------------------------------------------------------

@dataclass
class RunnerRow:
    track: str
    date: str       # YYYY-MM-DD
    race: int
    box: int
    runner: str
    trainer: str | None

# ---- Helpers ----------------------------------------------------------------

RACE_HDR = re.compile(r"\bRACE\s*(\d{1,2})\b", re.IGNORECASE)
# Common runner line variants found in AUS greyhound PDFs:
#  - "1  FAST DOG (Trainer: John Smith)"
#  - "1 FAST DOG T: John Smith"
#  - "Box 1 FAST DOG ... Trainer John Smith"
RUNNER_LINE = re.compile(
    r"(?:^|\s)(?:Box\s*)?([1-8])\s+([A-Za-z][A-Za-z0-9' .\-]+?)(?:\s{2,}|\s*\(|\s+T:|\s+Trainer[: ]|$)",
    re.IGNORECASE,
)
TRAINER_FALLBACKS: list[re.Pattern] = [
    re.compile(r"Trainer[: ]\s*([A-Za-z][A-Za-z .'\-]+)"),
    re.compile(r"T:\s*([A-Za-z][A-Za-z .'\-]+)"),
    re.compile(r"\(Trainer:\s*([A-Za-z][A-Za-z .'\-]+)\)"),
]

TRACK_DATE = re.compile(
    r"^\s*([A-Z]{3,5})\s*[_\-– ]\s*(20\d{2}[\-/\.]\d{2}[\-/\.]\d{2})\s*$"
)

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _find_trainer(snippet: str) -> str | None:
    for pat in TRAINER_FALLBACKS:
        m = pat.search(snippet)
        if m:
            return _norm(m.group(1))
    return None

# ---- Core -------------------------------------------------------------------

def _infer_track_date_from_filename(path: str) -> tuple[str, str] | None:
    name = os.path.basename(path).removesuffix(".pdf")
    m = TRACK_DATE.match(name)
    if m:
        track = m.group(1).upper()
        datestr = m.group(2).replace(".", "-").replace("/", "-")
        return track, datestr
    # fallback: QSTR_2025-09-08.pdf style (already)
    parts = name.split("_")
    if len(parts) >= 2 and re.match(r"20\d{2}-\d{2}-\d{2}", parts[-1]):
        return parts[0].upper(), parts[-1]
    return None

def parse_pdf_file(pdf_path: str) -> List[RunnerRow]:
    text = extract_text(pdf_path)
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    # Track/date from filename (most reliable)
    td = _infer_track_date_from_filename(pdf_path)
    track, datestr = (td if td else ("UNK", datetime.now().strftime("%Y-%m-%d")))
    rows: list[RunnerRow] = []

    race = None
    # Join adjacent “paragraphs” to keep local trainer info near the runner line
    for i, ln in enumerate(lines):
        ln_norm = _norm(ln)

        # Race header
        m_r = RACE_HDR.search(ln_norm)
        if m_r:
            race = int(m_r.group(1))
            continue

        # Detect runner lines (box, runner name)
        m = RUNNER_LINE.search(ln_norm)
        if m and race is not None:
            box = int(m.group(1))
            runner = _norm(m.group(2))

            # Look around this line for trainer
            context = " ".join(_norm(x) for x in lines[max(0, i-2): i+3])
            trainer = _find_trainer(context)

            rows.append(RunnerRow(
                track=track, date=datestr, race=race, box=box,
                runner=runner, trainer=trainer
            ))
    return rows

def parse_forms_for_today(forms_dir: str = "forms") -> List[RunnerRow]:
    # Today’s PDFs (e.g., QSTR_YYYY-MM-DD.pdf)
    today = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d")
    candidates = sorted(glob.glob(os.path.join(forms_dir, f"*_{today}.pdf")))
    rows: list[RunnerRow] = []
    for pdf in candidates:
        try:
            rows.extend(parse_pdf_file(pdf))
        except Exception as e:
            print(f"[warn] failed {pdf}: {e}")
    return rows

def to_dicts(rows: Iterable[RunnerRow]) -> List[Dict]:
    return [asdict(r) for r in rows]

if __name__ == "__main__":
    out = parse_forms_for_today()
    for r in out[:10]:
        print(r)

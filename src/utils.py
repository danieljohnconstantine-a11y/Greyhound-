from __future__ import annotations
import re
from pathlib import Path
from datetime import datetime, date
from dateutil import parser as dtparser

DATE_FMT = "%Y-%m-%d"

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def resolve_run_date(s: str | None) -> date:
    if s in (None, "", "today"):
        return date.today()
    return dtparser.parse(s).date()

def newest_form_for_date(forms_dir: Path, run_date: date) -> list[Path]:
    wanted = []
    ds = run_date.strftime("%Y-%m-%d")
    for p in sorted(forms_dir.glob("*.pdf")):
        if ds in p.name:
            wanted.append(p)
    if wanted:
        return wanted
    cutoff = datetime.now().timestamp() - 36*3600
    return [p for p in forms_dir.glob("*.pdf") if p.stat().st_mtime >= cutoff]

TRACK_CODE_MAP = {
    "RICH": "Richmond",
    "HEAL": "Healesville",
    "GAWL": "Gawler",
    "GRAF": "Grafton",
    "DRWN": "Darwin",
    "QSTR": "QSTR",  # update later if needed
}

BOX_RE = re.compile(r"^(?:Box\s*)?(\d{1})\b")
RACE_RE = re.compile(r"\bRACE\s*(\d{1,2})\b", re.I)
DIST_RE = re.compile(r"(\d{3,4})\s?m\b", re.I)
DATE_IN_NAME = re.compile(r"(20\d{2}-\d{2}-\d{2})")

def infer_track_and_date_from_name(pdf: Path) -> tuple[str | None, str | None]:
    base = pdf.stem
    parts = base.split("_")
    trk = TRACK_CODE_MAP.get(parts[0].upper(), None) if parts else None
    m = DATE_IN_NAME.search(base)
    d = m.group(1) if m else None
    return trk, d

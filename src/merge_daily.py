#!/usr/bin/env python3
"""
merge_daily.py
No parsing magic here — just build a deterministic summary of what was fetched.
It writes reports/latest/summary.md only when >=1 valid PDF is present.
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
from datetime import datetime


def build_summary(forms_dir: Path, out_dir: Path) -> int:
    pdfs = sorted([p for p in forms_dir.glob("*.pdf") if p.is_file() and p.stat().st_size > 10 * 1024])
    if not pdfs:
        print("[merge] no valid PDFs to summarise.")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    summary = out_dir / "summary.md"

    lines = []
    lines.append(f"# Greyhound forms — fetched on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%SZ')} (UTC)")
    lines.append("")
    lines.append("## Files")
    for p in pdfs:
        lines.append(f"- {p.name}  ({p.stat().st_size//1024} KB)")
    lines.append("")
    lines.append("> Note: This summary confirms valid form PDFs were fetched. Further modelling/parsing can be added later.")

    summary.write_text("\n".join(lines), encoding="utf-8")
    print(f"[merge] wrote {summary} (files={len(pdfs)})")
    return len(pdfs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--forms", required=True, help="Directory with PDFs")
    ap.add_argument("--out", required=True, help="Directory to write summary.md")
    args = ap.parse_args()

    count = build_summary(Path(args.forms), Path(args.out))
    if count <= 0:
        print("[merge] no data — failing job to avoid 'empty data' commits.")
        sys.exit(3)
    return 0


if __name__ == "__main__":
    sys.exit(main())

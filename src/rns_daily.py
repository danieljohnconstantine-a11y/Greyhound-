#!/usr/bin/env python3
"""
rns_daily.py
Thin wrapper for fetch_forms.py so the workflow has a single, clear “fetch” step.
Exits non-zero if no valid PDFs were saved, to prevent empty reports/commits.
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path

from fetch_forms import fetch_all


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", "--out-dir", dest="out_dir", default="forms", help="Directory to save PDFs")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)

    saved = fetch_all(out_dir)
    print(f"[rns_daily] valid PDFs saved: {saved}")
    if saved <= 0:
        print("[rns_daily] no valid forms found — failing job to avoid empty data.")
        sys.exit(2)
    return 0


if __name__ == "__main__":
    sys.exit(main())

# scraper.py
import os, sys, csv, datetime, requests
from pathlib import Path

# === 1) ADD/EDIT TODAY'S FORM LINKS HERE ===
URLS = [
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/HEALG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/CAPAG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/DRWNG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/GRAFG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/GAWLG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/QSTRG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/RICHG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/SALEG3108form.pdf",
]

def safe_name(url: str) -> str:
    return url.rsplit("/", 1)[-1] or f"form_{abs(hash(url))}.pdf"

def main() -> int:
    today = datetime.date.today().isoformat()
    out_dir = Path("forms") / today
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_path = out_dir / "download_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as fcsv:
        writer = csv.writer(fcsv)
        writer.writerow(["url", "saved_as", "status", "bytes"])

        for url in URLS:
            fname = safe_name(url)
            dest = out_dir / fname
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200 and r.content.startswith(b"%PDF"):
                    dest.write_bytes(r.content)
                    writer.writerow([url, str(dest), "OK", len(r.content)])
                    print(f"✓ Saved {fname} ({len(r.content)} bytes)")
                else:
                    writer.writerow([url, str(dest), f"BAD_STATUS_{r.status_code}", 0])
                    print(f"✗ Failed {fname}: HTTP {r.status_code}")
            except Exception as e:
                writer.writerow([url, str(dest), f"ERROR_{type(e).__name__}", 0])
                print(f"✗ Error {fname}: {e}")

    print(f"\nAll done. Files in: {out_dir}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

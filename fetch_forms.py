# fetch_forms.py
# Best-effort downloader for today's PDFs. Safe on failure.
import os, sys, datetime, time
from pathlib import Path
import requests

FORMS = [
    # Add/curate direct PDF URLs here (R&S often: /racing/raceinfo/newformpdf/<CODE>.pdf)
    # e.g. "https://files.racingandsports.com/racing/raceinfo/newformpdf/RICHG3108form.pdf",
]

def fetch(url, outdir: Path):
    name = url.split("/")[-1]
    out = outdir / name
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200 and r.headers.get("content-type","").lower().startswith("application/pdf"):
            out.write_bytes(r.content)
            print("OK ", name)
        else:
            print("SKIP", name, "status=", r.status_code)
    except Exception as e:
        print("ERR ", name, e)

def main():
    outdir = Path("forms")
    outdir.mkdir(exist_ok=True, parents=True)
    for u in FORMS:
        fetch(u, outdir)
        time.sleep(1.0)

if __name__ == "__main__":
    main()

import pathlib, re
from urllib.parse import urlparse
from .utils import OUT_BASE, write_bytes, utcstamp, write_json
from .http_client import fetch

FORMS_DIR = pathlib.Path("forms")
FORMS_DIR.mkdir(parents=True, exist_ok=True)

def _filename_from_url(u: str) -> str:
    path = urlparse(u).path
    name = pathlib.Path(path).name or "file.pdf"
    # normalize
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name

def _load_manifest(path: pathlib.Path):
    if not path.exists():
        return None
    import json
    return json.loads(path.read_text(encoding="utf-8"))

def main():
    ts = utcstamp()
    rns_manifest = _load_manifest(OUT_BASE/"rns"/sorted((OUT_BASE/"rns").glob("meetings_*.json"))[-1].name) if (OUT_BASE/"rns").exists() else None
    dogs_manifest = _load_manifest(OUT_BASE/"thedogs"/sorted((OUT_BASE/"thedogs").glob("meetings_*.json"))[-1].name) if (OUT_BASE/"thedogs").exists() else None

    downloaded = []
    errors = []

    # 1) RNS PDFs
    if rns_manifest and rns_manifest.get("pdfs"):
        for u in rns_manifest["pdfs"][:30]:   # avoid going crazy if index lists too many
            try:
                r = fetch(u)
                name = _filename_from_url(u)
                write_bytes(FORMS_DIR/name, r.content)
                downloaded.append(str(FORMS_DIR/name))
            except Exception as e:
                errors.append(f"{u} :: {e}")

    # 2) TheDogs sometimes links PDFs on track-day pages. If we have any, try them too.
    if dogs_manifest and dogs_manifest.get("meetings"):
        # we stored only meeting URLs; fetch each page and look for .pdf
        for m in dogs_manifest["meetings"][:20]:
            u = m["url"]
            try:
                r = fetch("https://www.thedogs.com.au" + u if u.startswith("/") else u)
                html = r.text
                for pdf in re.findall(r'href="([^"]+\.pdf)"', html, flags=re.I):
                    full = pdf if pdf.startswith("http") else "https://www.thedogs.com.au" + pdf
                    try:
                        rr = fetch(full)
                        name = _filename_from_url(full)
                        write_bytes(FORMS_DIR/name, rr.content)
                        downloaded.append(str(FORMS_DIR/name))
                    except Exception as ee:
                        errors.append(f"{full} :: {ee}")
            except Exception as e:
                errors.append(f"{u} :: {e}")

    write_json(OUT_BASE/"combined"/f"forms_fetch_{ts}.json", {
        "fetched_at_utc": ts,
        "downloaded": downloaded,
        "errors": errors,
        "count": len(downloaded),
    })

if __name__ == "__main__":
    main()

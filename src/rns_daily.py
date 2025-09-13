import re, pathlib
from bs4 import BeautifulSoup
from .http_client import fetch
from .utils import utcstamp, write_json, write_text, OUT_BASE

OUT_DIR = OUT_BASE / "rns"

INDEX_CANDIDATES = [
    # common public index pages where the PDFs are linked from
    "https://www.racingandsports.com.au/form-guide/greyhound",
    "https://www.racingandsports.com.au/form-guide",
]

def find_pdfs(html: str):
    soup = BeautifulSoup(html, "lxml")
    urls = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if ".pdf" in href and ("grey" in href or "form" in href or "newformpdf" in href):
            if href.startswith("/"):
                href = "https://www.racingandsports.com.au" + href
            urls.append(href)
    return sorted(set(urls))

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = utcstamp()

    all_pdfs = []
    status_map = {}
    debug_concat = []
    for url in INDEX_CANDIDATES:
        try:
            r = fetch(url)
            status_map[url] = r.status_code
            debug_concat.append(f"### {url}\n\n{r.text}\n")
            all_pdfs.extend(find_pdfs(r.text))
        except Exception as e:
            status_map[url] = f"ERROR: {e}"
            debug_concat.append(f"### {url}\n\nERROR: {e}\n")

    write_text(OUT_DIR / f"debug_{ts}.html", "\n\n".join(debug_concat))
    write_json(OUT_DIR / f"meetings_{ts}.json", {
        "fetched_at_utc": ts,
        "source": "rns",
        "status_by_url": status_map,
        "count": len(all_pdfs),
        "pdfs": all_pdfs,
    })

if __name__ == "__main__":
    main()

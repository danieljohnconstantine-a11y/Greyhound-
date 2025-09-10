# src/ladbrokes_auto.py
"""
Fetch Ladbrokes greyhound races and save to:
  data/ladbrokes_greyhounds_YYYY-MM-DD.json

Notes:
- Uses a public Ladbrokes endpoint commonly used for "next to go" greyhound racing.
- If it fails (e.g., endpoint changes), we still write a small JSON error stub
  so the workflow stays traceable and artifacts/commits still happen.
"""

from pathlib import Path
import datetime, json, requests

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.date.today().strftime("%Y-%m-%d")
outfile = DATA_DIR / f"ladbrokes_greyhounds_{today_str}.json"

URL = "https://api.ladbrokes.com.au/sportsbook-api/racing/next-to-go/GREYHOUND"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def main():
    try:
        print(f"[INFO] GET {URL}")
        r = requests.get(URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        outfile.write_text(json.dumps(data, indent=2), encoding="utf-8")
        print(f"[OK] wrote {outfile}")
    except Exception as e:
        stub = {"error": str(e), "source": URL, "date": today_str}
        outfile.write_text(json.dumps(stub, indent=2), encoding="utf-8")
        print(f"[WARN] wrote error stub to {outfile}: {e}")

if __name__ == "__main__":
    main()

import os
import json
import requests
from datetime import datetime
from bs4 import BeautifulSoup

def scrape_ladbrokes_greyhounds():
    """
    Scrapes Ladbrokes greyhound racing meetings.
    """
    url = "https://www.ladbrokes.com.au/racing/greyhound-racing"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    meetings = []

    # Look for greyhound meeting links
    for meeting in soup.select("a[href*='/racing/greyhound-racing/']"):
        name = meeting.get_text(strip=True)
        link = meeting.get("href")
        if name and link:
            if not link.startswith("http"):
                link = "https://www.ladbrokes.com.au" + link
            meetings.append({"name": name, "url": link})

    return meetings

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="data/ladbrokes", help="Output folder")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    meetings = scrape_ladbrokes_greyhounds()

    out_file = os.path.join(args.out_dir, f"ladbrokes_meetings_{datetime.utcnow().date()}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(meetings, f, indent=2)

    print(f"✅ Saved {len(meetings)} meetings to {out_file}")

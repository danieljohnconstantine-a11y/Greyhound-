import requests
from bs4 import BeautifulSoup
import os, json

def scrape_ladbrokes_greyhounds():
    url = "https://www.ladbrokes.com.au/racing/greyhound-racing"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.ladbrokes.com.au/",
        "Connection": "keep-alive",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    meetings = []

    # Example: adjust selectors after testing HTML
    for meeting in soup.select("a[href*='/racing/greyhound/']"):
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
    parser.add_argument("--out-dir", default="data/ladbrokes")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    meetings = scrape_ladbrokes_greyhounds()

    out_file = os.path.join(args.out_dir, "ladbrokes_meetings.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(meetings, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(meetings)} meetings → {out_file}")

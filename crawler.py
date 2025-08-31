import requests
from bs4 import BeautifulSoup

# Example: scrape Racing & Sports Greyhound form PDFs for a given date
BASE_URL = "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/"

# Replace with your date and track code
tracks = ["SALEG3108form.pdf", "RICHG3108form.pdf", "CAPAG3108form.pdf"]

for track in tracks:
    url = BASE_URL + track
    print(f"Fetching: {url}")
    r = requests.get(url)
    if r.status_code == 200:
        filename = track
        with open(filename, "wb") as f:
            f.write(r.content)
        print(f"Saved {filename}")
    else:
        print(f"Failed {track} → {r.status_code}")

# Greyhound-
Greyhound daily form guide 
import requests
from datetime import datetime

# List of form guide URLs for today's races
URLS = [
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/SALEG3108form.pdf",
    "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/RICHG3108form.pdf",
    # add all the others here...
]

today = datetime.now().strftime("%Y-%m-%d")

for url in URLS:
    filename = url.split("/")[-1]
    print(f"Downloading {filename}...")
    r = requests.get(url)
    if r.status_code == 200:
        with open(filename, "wb") as f:
            f.write(r.content)
        print(f"Saved {filename}")
    else:
        print(f"Failed to fetch {url}"

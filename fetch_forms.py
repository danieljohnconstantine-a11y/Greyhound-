import requests
from datetime import date

# List of tracks (we can add/remove as needed)
tracks = ["SALEG", "RICHG", "CAPAG", "HEALG", "DRWNG", "GRAFG", "GAWLG", "QSTRG"]

today = date.today().strftime("%d%m")  # e.g. "3108" for 31 August
year = date.today().strftime("%y")     # e.g. "25" for 2025

for track in tracks:
    url = f"https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/{track}{today}form.pdf"
    filename = f"{track}{today}.pdf"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            with open(filename, "wb") as f:
                f.write(r.content)
            print(f"✅ Saved {filename}")
        else:
            print(f"❌ Could not fetch {url}")
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")

import requests
import json
import datetime
import os

# Base URL for Ladbrokes greyhound races (this may need adjusting if site structure changes)
BASE_URL = "https://api.ladbrokes.com.au/sportsbook-api/racing/next-to-go/GREYHOUND"

# Output folder
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_greyhound_meetings():
    """Fetch upcoming greyhound meetings from Ladbrokes"""
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching Ladbrokes data: {e}")
        return None

def save_meetings(data):
    """Save Ladbrokes greyhound meetings to JSON file"""
    if not data:
        print("⚠️ No data to save.")
        return

    today = datetime.date.today().strftime("%Y-%m-%d")
    filename = os.path.join(OUTPUT_DIR, f"ladbrokes_greyhounds_{today}.json")

    with open(filename, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Saved Ladbrokes greyhound meetings to {filename}")

def main():
    print("Fetching Ladbrokes greyhound meetings...")
    data = fetch_greyhound_meetings()
    save_meetings(data)

if __name__ == "__main__":
    main()

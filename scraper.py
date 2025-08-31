import requests

# Example URL – replace with the PDF or HTML form guide link for today
url = "https://files.racingandsports.com/racing/racing/raceinfo/newformpdf/SALEG3108form.pdf"

response = requests.get(url)

with open("formguide.pdf", "wb") as f:
    f.write(response.content)

print("Downloaded form guide to formguide.pdf")

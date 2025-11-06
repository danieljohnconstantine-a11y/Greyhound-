# Column Template Required

I cannot access Google Sheets directly due to network restrictions in my environment.

## Please provide the column headers using ONE of these methods:

### Option 1: Copy-paste the headers (EASIEST)
Open your Google Sheet, select the header row, copy it, and paste it in a comment like this:
```
Track,RaceNo,Date,Time,Distance,Class,Prize,Box,DogName,FormNumbers,Color,Age,Sex,Weight,Trainer,Owner,Sire,Dam,CareerWins,CareerPlaces,CareerShows,CareerPrize,RTC,DLR,DLW,...
```

### Option 2: Download and upload the file
1. In Google Sheets, go to File → Download → CSV
2. Upload the CSV file to this repository in the `/data` folder
3. Comment with the file path

### Option 3: Create the file directly
Run this command in your local repository:
```bash
# Copy the first row from your spreadsheet
echo "Track,RaceNo,Date,..." > data/column_template.csv
git add data/column_template.csv
git commit -m "Add column template"
git push
```

## What I'll do once I have the columns:
1. Update the parser to extract all 61+ fields
2. Map them to your exact column names and order
3. Maintain Race/Box ordering (Race 1 Box 1 → Race 1 Box 2, etc.)
4. Generate identical CSV and XLSX outputs
5. Test with sample PDFs

## Currently identified fields (61):
I've already analyzed the PDFs and can extract:
- Race info: Track, RaceNo, Date, Time, Distance, Class, Prize
- Dog basics: Box, Name, Form, Color, Age, Sex, Weight
- Connections: Trainer, Owner, Sire, Dam
- Career: Wins, Places, Shows, Prize, RTC, DLR, DLW
- Splits: j50s, j350s, t50s, t350s
- Performance: CarPM/s, API, RTC/km, etc.
- Records: G1-G3, FU, 2U, 3U, track conditions
- Stats: JT percentages
- Comments

Ready to implement once I have your exact column specification!

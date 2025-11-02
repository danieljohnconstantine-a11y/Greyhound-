# Identified Fields from Greyhound Race Form PDFs

Based on analysis of the Racing & Sports PDF format, I've identified **61 extractable fields** across the following categories:

## 1. Basic Race Information (10 fields)
- Track
- RaceNo
- RaceDate
- RaceTime
- Distance
- RaceClass
- PrizeMoney
- Box
- DogName
- FormNumbers

## 2. Dog Details (12 fields)
- Age
- Sex
- Weight
- BoxPosition
- Trainer
- Career_Wins
- Career_Places
- Career_Shows
- CareerPrize
- RTC (Recent Track Condition)
- DLR (Days Last Run)
- DLW (Days Last Win)

## 3. Breeding Information (3 fields)
- Sire
- Dam
- Owner

## 4. Performance Statistics (14 fields)
- j50s (Jump 50m split)
- j350s (Jump 350m split)
- t50s (Trial 50m split)
- t350s (Trial 350m split)
- RacedDistance
- WinningDistance
- CarPM_per_s (Career Prize Money per start)
- 12mPM_per_s (12 month Prize Money per start)
- API (Ability Performance Index)
- RTC_per_km (Recent Track Condition per km)
- RDistTC (Recent Distance Track Condition)
- DLS (Days Last Start)
- DLW_stat (Days Last Win statistic)
- DOD (Days On Distance)

## 5. Career Records (6 fields)
- Car_total (Career total)
- 12m_total (12 month total)
- Crs_total (Course total)
- Dist_total (Distance total)
- ClockW (Clock Wins)
- AClockW (Adjusted Clock Wins)

## 6. Track Condition Records (13 fields)
- G1_record (Grade 1)
- G2_record (Grade 2)
- G3_record (Grade 3)
- LR_record (Long Run)
- FU_record (First Up)
- 2U_record (Second Up)
- 3U_record (Third Up)
- Firm_record
- Good_record
- Soft_record
- Heavy_record
- AW_record (All Weather)
- Turf_record

## 7. Jockey/Trainer Statistics (2 fields)
- JT_percentage_1
- JT_percentage_2

## 8. Comments (1 field)
- Comments

**Total: 61 fields**

## Current Status
The existing parser extracts only 11 fields:
- Track, RaceNo, Distance, Box, DogName, Trainer, PrizeMoney, Odds, Margins, Comment, RaceTime

## Next Steps
Awaiting Excel template with exact column headers and order to implement the enhanced parser that extracts all 61+ fields.

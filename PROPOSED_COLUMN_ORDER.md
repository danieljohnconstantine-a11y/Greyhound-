# Proposed Column Order for Enhanced Greyhound Parser

## 61 Columns in Suggested Order

This is a proposed column order based on typical greyhound form analysis workflows. Please provide your Excel template if you need a different order.

### Race Identification (5 columns)
1. Track
2. RaceNo
3. RaceDate
4. RaceTime
5. Distance

### Race Details (2 columns)
6. RaceClass
7. TotalPrizeMoney

### Dog Identification (4 columns)
8. Box
9. DogName
10. FormNumbers
11. Color

### Dog Physical (3 columns)
12. Age
13. Sex
14. Weight

### Dog Connections (3 columns)
15. Trainer
16. Owner
17. Sire

### Dam (1 column)
18. Dam

### Career Summary (4 columns)
19. Career_Wins
20. Career_Places
21. Career_Shows
22. CareerPrizeMoney

### Recent Performance (3 columns)
23. RTC (Recent Track Condition)
24. DLR (Days Last Run)
25. DLW (Days Last Win)

### Split Times (4 columns)
26. j50s
27. j350s
28. t50s
29. t350s

### Distance Performance (2 columns)
30. RacedDistance
31. WinningDistance

### Performance Metrics (8 columns)
32. CarPM_per_s
33. 12mPM_per_s
34. API
35. RTC_per_km
36. RDistTC
37. DLS
38. DLW_stat
39. DOD

### Career Records (6 columns)
40. Car_Record
41. 12m_Record
42. Crs_Record
43. Dist_Record
44. ClockW
45. AClockW

### Grade Records (4 columns)
46. G1_Record
47. G2_Record
48. G3_Record
49. LR_Record

### Position Records (3 columns)
50. FU_Record (First Up)
51. 2U_Record (Second Up)
52. 3U_Record (Third Up)

### Track Surface Records (6 columns)
53. Firm_Record
54. Good_Record
55. Soft_Record
56. Heavy_Record
57. AW_Record
58. Turf_Record

### Trainer/Jockey Stats (2 columns)
59. JT_Percentage_1
60. JT_Percentage_2

### Additional Info (1 column)
61. Comments

---

## Notes:
- Column names can be customized to match your Excel template
- Order can be rearranged as needed
- Some fields may be empty for certain PDFs or dogs
- The parser will extract all available data and leave blanks where data is not present

## To Customize:
Please provide your Excel template file, or list the exact column names and order you want, and I'll update the parser accordingly.

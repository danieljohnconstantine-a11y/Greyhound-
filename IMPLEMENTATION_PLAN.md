# Enhanced Parser Implementation Plan

## Overview
Expanding parser from 11 fields to 61+ fields for comprehensive greyhound race data extraction.

## Current Capabilities
The existing `parser_step1_complete_layoutaware.py` extracts:
- ✅ Track, RaceNo, Distance, Box, DogName, Trainer
- ✅ PrizeMoney, Margins (career record)
- ✅ RaceTime (for some formats)
- ✅ Handles multiple PDF formats (standard Racing & Sports + Broken Hill)
- ✅ Maintains race/box ordering
- ✅ Outputs to CSV and XLSX

## Enhancement Required

### 1. Field Expansion
Need to extract **50 additional fields** from the detailed dog sections:

#### Currently Missing:
- **Race Details**: RaceDate, RaceTime, RaceClass
- **Dog Physical**: Age, Sex, Weight, Color, FormNumbers
- **Breeding**: Sire, Dam, Owner
- **Split Times**: j50s, j350s, t50s, t350s
- **Performance Metrics**: CarPM/s, 12mPM/s, API, RTC/km, RDistTC, DLS, DLW, DOD
- **Career Records**: Car_Record, 12m_Record, Crs_Record, Dist_Record, ClockW, AClockW
- **Track Conditions**: G1-G3, LR, FU, 2U, 3U records
- **Surface Records**: Firm, Good, Soft, Heavy, AW, Turf
- **Stats**: JT percentages
- **Additional**: RacedDistance, WinningDistance, Comments

### 2. Data Extraction Strategy

#### Phase 1: Summary Line Extraction
Extract from the compact summary lines (already working):
```
1. Jimmy Legs 1d 0.0kg 1 Mark Saal 0 - 0 - 0 $0 FU 0 Mdn
```
Fields: Box, DogName, Age/Sex, Weight, BoxPosition, Trainer, Career (W-P-S), Prize, RTC, DLR/DLW

#### Phase 2: Detailed Section Extraction (NEW)
Extract from detailed dog sections:
```
JIMMY LEGS j50s j350s t50s t350s
1. 0kg (1) bdl 1 D MARK SAAL Horse: First Ride - - 7-14-50 80-116-
AUSSIE INFRARED (AUS) - BABS KRAMER (AUS) J/T: 14%-42% 350
Raced Distance: 0-0 Winning Distance: NA 23%-56%
Owner: Mark Saal
CarPM/s 12mPM/s API RTC/km RDistTC DLS DLW DOD Car 12m Crs Dist ClockW AClockW
$0 $0 0.0 FU/0 0 0 0 FU - - - - - -
G1 G2 G3 LR FU 2U 3U Firm Good Soft Heavy AW Turf
- - - - - - - - - - - - -
```

### 3. Implementation Steps

#### Step 1: Receive Excel Template ⏳
- Get exact column names and order
- Confirm field mappings
- Identify any custom calculations needed

#### Step 2: Create Enhanced Parser
- Add new extraction functions for each field category
- Extend `parse_page_layout_aware()` to capture detailed sections
- Map all extracted data to template columns

#### Step 3: Update Output Structure
- Replace current 11-column output with 61-column structure
- Ensure proper race/box ordering
- Validate CSV and XLSX have identical data

#### Step 4: Testing
- Test with all PDF formats (standard, Broken Hill, etc.)
- Verify all fields populate correctly
- Check handling of missing data
- Confirm ordering: Race 1 Box 1 → Race 1 Box 2 → ... → Race 2 Box 1

#### Step 5: Documentation
- Update PIPELINE_README.md with new field descriptions
- Add field mapping documentation
- Include sample output

## Technical Considerations

### Parsing Challenges
1. **Multi-line data**: Dog details span 8-10 lines in PDF
2. **Varying formats**: Different PDFs may have slightly different layouts
3. **Missing data**: Not all dogs have all fields (e.g., "First Ride" dogs)
4. **Data alignment**: Columns in PDF may not align perfectly

### Solutions
- Use context-aware parsing (look for section headers)
- Regex patterns for each field type
- Default values for missing data (empty string or "N/A")
- Layout-aware text extraction (pdfplumber)

## Expected Timeline
1. **Template received**: Day 0
2. **Parser enhanced**: Day 1
3. **Testing complete**: Day 2
4. **Ready for production**: Day 3

## Output Preview
```csv
Track,RaceNo,RaceDate,RaceTime,Distance,RaceClass,TotalPrizeMoney,Box,DogName,FormNumbers,Color,Age,Sex,Weight,Trainer,Owner,Sire,Dam,Career_Wins,Career_Places,Career_Shows,CareerPrizeMoney,RTC,DLR,DLW,j50s,j350s,t50s,t350s,RacedDistance,WinningDistance,CarPM_per_s,12mPM_per_s,API,RTC_per_km,RDistTC,DLS,DLW_stat,DOD,Car_Record,12m_Record,Crs_Record,Dist_Record,ClockW,AClockW,G1_Record,G2_Record,G3_Record,LR_Record,FU_Record,2U_Record,3U_Record,Firm_Record,Good_Record,Soft_Record,Heavy_Record,AW_Record,Turf_Record,JT_Percentage_1,JT_Percentage_2,Comments
CAPA,7,2025-09-07,12:05PM,366,Maiden,2700,1,Jimmy Legs,,bdl,1,d,0.0,Mark Saal,Mark Saal,AUSSIE INFRARED,BABS KRAMER,0,0,0,0,FU,0,0,7,80,14,116,0-0,NA,0,0,0.0,FU/0,0,0,0,FU,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,14%-42%,23%-56%,First Ride
...
```

## Next Steps
**Awaiting Excel template to proceed with implementation.**

Once template is provided, I will:
1. Map all fields to exact column names
2. Implement enhanced extraction logic
3. Test with sample PDFs
4. Deliver working parser

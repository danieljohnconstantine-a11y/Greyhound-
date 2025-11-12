#!/usr/bin/env python3
"""
export_to_excel.py
==================
Excel export module for greyhound racing data.

Generates structured Excel workbooks with two sheets:
1. Dog Summary - Overview statistics for each dog
2. Race History Detail - Detailed race-by-race history

Usage:
    from export_to_excel import export_to_excel
    export_to_excel(summary_df, history_df, output_path)
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows


def create_dog_summary_df(parsed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create Dog Summary DataFrame with 32 columns (Groups A & B combined).
    
    Note: This version creates a template with available data from basic PDF parsing.
    Enhanced PDF parsing would be needed to populate all fields with actual data.
    
    Args:
        parsed_df: Basic parsed data with columns [track, date, race, box, runner]
    
    Returns:
        DataFrame with Dog Summary columns
    """
    if parsed_df.empty:
        # Return empty DataFrame with correct columns
        columns = [
            'Race_No', 'Career_W-P-S', 'Avg_Speed_km/h', 'Dog_Name', 'Prize_Money', 'Min_Speed_km/h',
            'Tab_No', 'RTC', 'Max_Speed_km/h', 'FF_Form', 'DLR', 'BP', 'DLW', 'A/S', 'Car_PM/s (G1)', 
            'WT (kg)', '12m_PM/s (G2)', 'Trainer', 'API (G3)', 'Sire', 'RTC/km', 'Dam', 'Trainer_Win_%',
            'Owner', 'Trainer_Place_%', 'Raced_Dist_W-P-S', 'Crs_W-P-S', 'Dist_W-P-S', 'FU_W-P-S',
            '2U_W-P-S', 'DOD'
        ]
        return pd.DataFrame(columns=columns)
    
    # Group by dog and race to create summary
    summary_rows = []
    
    for (track, date, race), race_group in parsed_df.groupby(['track', 'date', 'race']):
        for _, dog in race_group.iterrows():
            # Create row with available data
            # Note: Many fields require enhanced PDF parsing to extract actual values
            row = {
                'Race_No': f"{track} R{race}",
                'Career_W-P-S': 'N/A',  # Needs historical data parsing
                'Avg_Speed_km/h': 'N/A',  # Needs speed data from PDF
                'Dog_Name': dog['runner'],
                'Prize_Money': 'N/A',  # Needs prize data from PDF
                'Min_Speed_km/h': 'N/A',  # Needs speed data
                'Tab_No': str(dog['box']),
                'RTC': 'N/A',  # Needs race timing data
                'Max_Speed_km/h': 'N/A',  # Needs speed data
                'FF_Form': 'N/A',  # Needs form data from PDF
                'DLR': 'N/A',  # Needs detailed race info
                'BP': str(dog['box']),  # Box position
                'DLW': 'N/A',  # Needs weight data
                'A/S': 'N/A',  # Needs age/sex data from PDF
                'Car_PM/s (G1)': 'N/A',  # Needs prize money calculation
                'WT (kg)': 'N/A',  # Needs weight from PDF
                '12m_PM/s (G2)': 'N/A',  # Needs 12-month data
                'Trainer': 'N/A',  # Needs trainer data from PDF
                'API (G3)': 'N/A',  # Needs API calculation
                'Sire': 'N/A',  # Needs pedigree data from PDF
                'RTC/km': 'N/A',  # Needs race timing
                'Dam': 'N/A',  # Needs pedigree data
                'Trainer_Win_%': 'N/A',  # Needs trainer statistics
                'Owner': 'N/A',  # Needs owner data from PDF
                'Trainer_Place_%': 'N/A',  # Needs trainer statistics
                'Raced_Dist_W-P-S': 'N/A',  # Needs distance statistics
                'Crs_W-P-S': 'N/A',  # Needs course statistics
                'Dist_W-P-S': 'N/A',  # Needs distance statistics
                'FU_W-P-S': 'N/A',  # Needs fast/up statistics
                '2U_W-P-S': 'N/A',  # Needs 2-up statistics
                'DOD': date  # Date of data
            }
            summary_rows.append(row)
    
    return pd.DataFrame(summary_rows)


def create_race_history_df(parsed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create Race History Detail DataFrame with 22 columns (Group C).
    
    Note: This version creates a template with available data from basic PDF parsing.
    Enhanced PDF parsing would be needed to populate all fields with actual data.
    
    Args:
        parsed_df: Basic parsed data with columns [track, date, race, box, runner]
    
    Returns:
        DataFrame with Race History Detail columns
    """
    if parsed_df.empty:
        # Return empty DataFrame with correct columns
        columns = [
            'Dog_Name', 'Tab_No', 'Hist_Date', 'Hist_Track', 'Hist_Distance', 'Hist_Finish_Pos',
            'Hist_Margin_L', 'Hist_Race_Time', 'Hist_Sec_Time', 'Hist_Sec_Time_Adj',
            'Hist_Speed_km/h', 'Hist_SOT', 'Hist_RST', 'Hist_BP', 'Hist_Odds', 'Hist_API',
            'Hist_Prize_Won', 'Hist_Winner', 'Hist_2nd_Place', 'Hist_3rd_Place',
            'Hist_Settled_Turn', 'Hist_Ongoing_Winners', 'Hist_Track_Direction'
        ]
        return pd.DataFrame(columns=columns)
    
    # Create race history entries
    history_rows = []
    
    for _, dog in parsed_df.iterrows():
        # Create history row with available data
        # Note: Most fields require enhanced PDF parsing to extract actual historical race data
        row = {
            'Dog_Name': dog['runner'],
            'Tab_No': str(dog['box']),
            'Hist_Date': dog['date'],
            'Hist_Track': dog['track'],
            'Hist_Distance': 'N/A',  # Needs distance from PDF
            'Hist_Finish_Pos': 'N/A',  # Needs result data from PDF
            'Hist_Margin_L': 'N/A',  # Needs margin data
            'Hist_Race_Time': 'N/A',  # Needs timing data
            'Hist_Sec_Time': 'N/A',  # Needs sectional timing
            'Hist_Sec_Time_Adj': 'N/A',  # Needs adjusted timing
            'Hist_Speed_km/h': 'N/A',  # Needs speed calculation
            'Hist_SOT': 'N/A',  # Needs SOT data
            'Hist_RST': 'N/A',  # Needs RST data
            'Hist_BP': str(dog['box']),  # Box position
            'Hist_Odds': 'N/A',  # Needs odds data from PDF
            'Hist_API': 'N/A',  # Needs API data
            'Hist_Prize_Won': 'N/A',  # Needs prize data
            'Hist_Winner': 'N/A',  # Needs race result
            'Hist_2nd_Place': 'N/A',  # Needs race result
            'Hist_3rd_Place': 'N/A',  # Needs race result
            'Hist_Settled_Turn': 'N/A',  # Needs position data
            'Hist_Ongoing_Winners': 'N/A',  # Needs ongoing data
            'Hist_Track_Direction': 'N/A'  # Needs track info from PDF
        }
        history_rows.append(row)
    
    return pd.DataFrame(history_rows)


def export_to_excel(summary_df: pd.DataFrame, history_df: pd.DataFrame, output_path: str | Path) -> None:
    """
    Export greyhound data to structured Excel file with two sheets.
    
    Args:
        summary_df: Dog Summary DataFrame (32 columns)
        history_df: Race History Detail DataFrame (22 columns)
        output_path: Path to save Excel file
    
    Raises:
        ValueError: If DataFrames don't have expected columns
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create workbook
    wb = Workbook()
    
    # Remove default sheet
    if 'Sheet' in wb.sheetnames:
        wb.remove(wb['Sheet'])
    
    # Sheet 1: Dog Summary
    ws_summary = wb.create_sheet("Dog Summary", 0)
    
    # Write Dog Summary data
    if not summary_df.empty:
        for r_idx, row in enumerate(dataframe_to_rows(summary_df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws_summary.cell(row=r_idx, column=c_idx, value=value)
                
                # Style header row
                if r_idx == 1:
                    cell.font = Font(bold=True, size=11)
                    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                    cell.font = Font(bold=True, size=11, color="FFFFFF")
                    cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Auto-adjust column widths
        for column in ws_summary.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_summary.column_dimensions[column_letter].width = adjusted_width
    
    # Sheet 2: Race History Detail
    ws_history = wb.create_sheet("Race History Detail", 1)
    
    # Write Race History data
    if not history_df.empty:
        for r_idx, row in enumerate(dataframe_to_rows(history_df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws_history.cell(row=r_idx, column=c_idx, value=value)
                
                # Style header row
                if r_idx == 1:
                    cell.font = Font(bold=True, size=11)
                    cell.fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
                    cell.font = Font(bold=True, size=11, color="FFFFFF")
                    cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Auto-adjust column widths
        for column in ws_history.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws_history.column_dimensions[column_letter].width = adjusted_width
    
    # Save workbook
    wb.save(output_path)
    print(f"[excel] Exported to {output_path}")
    print(f"[excel] - Dog Summary: {len(summary_df)} rows")
    print(f"[excel] - Race History Detail: {len(history_df)} rows")


if __name__ == "__main__":
    # Test the export function
    import sys
    
    # Create sample data
    sample_data = pd.DataFrame([
        {"track": "SALE", "date": "2025-09-01", "race": 1, "box": 1, "runner": "FAST DOG"},
        {"track": "SALE", "date": "2025-09-01", "race": 1, "box": 2, "runner": "QUICK PUP"},
        {"track": "RICH", "date": "2025-09-01", "race": 1, "box": 1, "runner": "SPEEDY ONE"},
    ])
    
    summary_df = create_dog_summary_df(sample_data)
    history_df = create_race_history_df(sample_data)
    
    output = "data/output/test_greyhound_results.xlsx"
    export_to_excel(summary_df, history_df, output)
    print(f"\nTest export created: {output}")

#!/usr/bin/env python3
"""
Script to analyze the event Excel file structure
"""

import pandas as pd
import sys

def analyze_excel_file(file_path):
    """Analyze the structure of an Excel file and print details."""
    try:
        # Read all sheets
        print(f"Analyzing file: {file_path}")
        excel_data = pd.read_excel(file_path, sheet_name=None)
        
        print(f"Number of sheets: {len(excel_data)}")
        print(f"Sheet names: {list(excel_data.keys())}")
        
        # Analyze each sheet
        for sheet_name, df in excel_data.items():
            print(f"\n{'=' * 50}")
            print(f"SHEET: {sheet_name}")
            print(f"{'=' * 50}")
            
            print(f"Shape: {df.shape} (rows, columns)")
            print(f"Columns: {df.columns.tolist()}")
            
            # Print sample data
            print("\nSample data (first 3 rows):")
            print(df.head(3).to_string())
            
            # Check for NaN values
            nan_count = df.isna().sum().sum()
            print(f"\nNumber of NaN values: {nan_count}")
            
            # Data types
            print("\nData types:")
            print(df.dtypes)
    
    except Exception as e:
        print(f"Error analyzing file: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "/home/ubuntu/upload/PROGRAMMAPROVVISORIO2025.xlsx"
    
    analyze_excel_file(file_path)

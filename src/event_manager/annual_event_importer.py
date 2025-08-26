import os
import pandas as pd
import sqlite3
from datetime import datetime

class AnnualEventImporter:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = self.conn.cursor()

    def import_events_from_excel(self, excel_path):
        df = pd.read_excel(excel_path)
        
        # Rename columns to match database schema (case-insensitive and handle spaces/special chars)
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(".", "").str.replace("/", "_")
        
        # Map Excel columns to database columns
        column_mapping = {
            'periodo': 'periodo',
            'accordo': 'accordo',
            'n_eventi': 'num_eventi', # Assuming 'n_eventi' in Excel maps to 'num_eventi' in DB
            'data': 'data',
            'expo_periodo': 'expo_periodo',
            'nome': 'nome',
            'mezzo_trasporto': 'mezzo_trasporto',
            'disciplina': 'disciplina',
            'localita': 'localita',
            'regione': 'regione',
            'expo_brand': 'expo_brand',
            'addetto': 'addetto',
            'pernotto': 'pernotto',
            'vitto_alloggio': 'vitto_alloggio',
            'treno': 'treno',
            'spazio_varie': 'spazio_varie',
            'incassi_2024': 'incassi_2024',
            'caschi': 'caschi',
            'occhiali': 'occhiali',
            'pneumatici': 'pneumatici',
            'bdg_incassi': 'bdg_incassi',
            'bdg_costi': 'bdg_costi',
            'km': 'km',
            'gasolio': 'gasolio',
            'autostrada': 'autostrada',
            'costi_reali': 'costi_reali',
            'incassi': 'incassi',
            'pos': 'pos',
            'cash': 'cash',
            'extra': 'extra',
            'vendita_privati_agenti': 'vendita_privati_agenti',
            'ffwd': 'ffwd'
        }

        # Filter DataFrame to only include columns present in the mapping and in the Excel file
        df_to_insert = df[[col for col in column_mapping.keys() if col in df.columns]].copy()
        df_to_insert.rename(columns=column_mapping, inplace=True)

        # Prepare data for insertion
        # Ensure all expected columns are present, fill missing with None or default values
        expected_columns = list(column_mapping.values())
        for col in expected_columns:
            if col not in df_to_insert.columns:
                df_to_insert[col] = None

        # Reorder columns to match the database table exactly
        df_to_insert = df_to_insert[expected_columns]

        # Insert data into the database
        for index, row in df_to_insert.iterrows():
            placeholders = ", ".join(["?" for _ in row])
            columns = ", ".join(row.index)
            sql = f"INSERT INTO annual_events ({columns}) VALUES ({placeholders})"
            try:
                self.cursor.execute(sql, tuple(row))
            except sqlite3.IntegrityError as e:
                print(f"Skipping duplicate entry or integrity error: {e} for row: {row.to_dict()}")
            except Exception as e:
                print(f"Error inserting row: {e} for row: {row.to_dict()}")
        self.conn.commit()
        print(f"Imported {len(df_to_insert)} rows into annual_events table.")


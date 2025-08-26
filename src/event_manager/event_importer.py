import pandas as pd
from datetime import datetime

class EventImporter:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = self.conn.cursor()

    def import_events_from_excel(self, file_path):
        df = pd.read_excel(file_path, header=0)
        df.columns = [str(col).strip().lower().replace(" ", "_").replace("°", "num").replace("&", "and") for col in df.columns]

        # Correctly identify and rename columns based on the provided Excel file structure
        column_mapping = {
            'periodo': 'periodo',
            'accordi': 'accordo',
            'n°_eventi': 'num_eventi',
            'data': 'data',
            'expo': 'expo_periodo',
            'nome': 'nome',
            'mezzo_trasporto': 'mezzo_trasporto',
            'disciplina': 'disciplina',
            'località': 'localita',
            'regione': 'regione',
            'expo.1': 'expo_brand',  # Assuming the second EXPO column is expo.1
            'addetto': 'addetto',
            'pernotto': 'pernotto',
            'vitto_and_alloggio': 'vitto_alloggio',
            'treno': 'treno',
            'spazio/varie': 'spazio_varie',
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
            'vendita_privati_/_agenti': 'vendita_privati_agenti',
            'ffwd': 'ffwd'
        }
        df.rename(columns=column_mapping, inplace=True)

        for index, row in df.iterrows():
            # Handle date conversion with mixed formats
            if isinstance(row.get("data"), datetime):
                event_date = row.get("data").strftime("%Y-%m-%d")
            else:
                event_date = str(row.get("data"))

            # Prepare data for insertion, ensuring all columns are present
            event_data = {
                "periodo": row.get("periodo"),
                "accordo": row.get("accordo"),
                "num_eventi": row.get("num_eventi"),
                "data": event_date,
                "expo_periodo": row.get("expo_periodo"),
                "nome": row.get("nome"),
                "mezzo_trasporto": row.get("mezzo_trasporto"),
                "disciplina": row.get("disciplina"),
                "localita": row.get("localita"),
                "regione": row.get("regione"),
                "expo_brand": row.get("expo_brand"),
                "addetto": row.get("addetto"),
                "pernotto": row.get("pernotto"),
                "vitto_alloggio": row.get("vitto_alloggio"),
                "treno": row.get("treno"),
                "spazio_varie": row.get("spazio_varie"),
                "incassi_2024": row.get("incassi_2024"),
                "caschi": row.get("caschi"),
                "occhiali": row.get("occhiali"),
                "pneumatici": row.get("pneumatici"),
                "bdg_incassi": row.get("bdg_incassi"),
                "bdg_costi": row.get("bdg_costi"),
                "km": row.get("km"),
                "gasolio": row.get("gasolio"),
                "autostrada": row.get("autostrada"),
                "costi_reali": row.get("costi_reali"),
                "incassi": row.get("incassi"),
                "pos": row.get("pos"),
                "cash": row.get("cash"),
                "extra": row.get("extra"),
                "vendita_privati_agenti": row.get("vendita_privati_agenti"),
                "ffwd": row.get("ffwd")
            }

            columns = ", ".join(event_data.keys())
            placeholders = ", ".join(["?" for _ in event_data.values()])
            sql = f"INSERT INTO events ({columns}) VALUES ({placeholders})"
            self.cursor.execute(sql, tuple(event_data.values()))

        self.conn.commit()
        print(f"Imported {len(df)} events.")



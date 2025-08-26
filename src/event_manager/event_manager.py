import pandas as pd
from datetime import datetime
import os

class EventManager:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = self.conn.cursor()
        self._create_event_table()

    def _create_event_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                periodo TEXT,
                accordo TEXT,
                num_eventi INTEGER,
                data TEXT,
                expo_periodo TEXT,
                nome TEXT,
                mezzo_trasporto TEXT,
                disciplina TEXT,
                localita TEXT,
                regione TEXT,
                expo_brand TEXT,
                addetto TEXT,
                pernotto REAL,
                vitto_alloggio REAL,
                treno REAL,
                spazio_varie REAL,
                incassi_2024 REAL,
                caschi REAL,
                occhiali REAL,
                pneumatici REAL,
                bdg_incassi REAL,
                bdg_costi REAL,
                km REAL,
                gasolio REAL,
                autostrada REAL,
                costi_reali REAL,
                incassi REAL,
                pos REAL,
                cash REAL,
                extra REAL,
                vendita_privati_agenti REAL,
                ffwd TEXT
            )
        """)
        self.conn.commit()

    def get_all_events(self):
        self.cursor.execute("SELECT * FROM events")
        return self.cursor.fetchall()

    def get_event_by_name(self, name):
        self.cursor.execute("SELECT * FROM events WHERE nome = ?", (name,))
        return self.cursor.fetchone()

    def update_event_data(self, event_id, column, value):
        sql = f"UPDATE events SET {column} = ? WHERE id = ?"
        self.cursor.execute(sql, (value, event_id))
        self.conn.commit()

    def delete_event(self, event_id):
        self.cursor.execute("DELETE FROM events WHERE id = ?", (event_id,))
        self.conn.commit()

    def get_event_statistics(self):
        # Example statistics: total income, total costs, number of events per region
        self.cursor.execute("SELECT SUM(incassi) FROM events")
        total_incassi = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT SUM(costi_reali) FROM events")
        total_costi = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT regione, COUNT(*) FROM events GROUP BY regione")
        events_per_region = self.cursor.fetchall()

        return {
            "total_incassi": total_incassi,
            "total_costi": total_costi,
            "events_per_region": events_per_region
        }



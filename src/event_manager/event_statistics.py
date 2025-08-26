import pandas as pd
import sqlite3

class EventStatistics:
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = self.conn.cursor()

    def get_total_incassi_by_year(self, year):
        self.cursor.execute("SELECT SUM(incassi) FROM annual_events WHERE SUBSTR(data, 1, 4) = ?", (str(year),))
        result = self.cursor.fetchone()[0]
        return result if result is not None else 0.0

    def get_total_costi_by_year(self, year):
        self.cursor.execute("SELECT SUM(costi_reali) FROM annual_events WHERE SUBSTR(data, 1, 4) = ?", (str(year),))
        result = self.cursor.fetchone()[0]
        return result if result is not None else 0.0

    def get_events_count_by_region(self, year=None):
        if year:
            self.cursor.execute("SELECT regione, COUNT(*) FROM annual_events WHERE SUBSTR(data, 1, 4) = ? GROUP BY regione", (str(year),))
        else:
            self.cursor.execute("SELECT regione, COUNT(*) FROM annual_events GROUP BY regione")
        return self.cursor.fetchall()

    def get_top_performing_events(self, year, top_n=5):
        self.cursor.execute("SELECT nome, incassi FROM annual_events WHERE SUBSTR(data, 1, 4) = ? ORDER BY incassi DESC LIMIT ?", (str(year), top_n))
        return self.cursor.fetchall()

    def get_product_sales_by_year(self, year):
        # This would require more detailed product-level data in annual_events or joining with products table
        # For now, we\'ll return a placeholder or focus on the existing columns
        self.cursor.execute("SELECT SUM(caschi), SUM(occhiali), SUM(pneumatici) FROM annual_events WHERE SUBSTR(data, 1, 4) = ?", (str(year),))
        result = self.cursor.fetchone()
        return {
            "caschi": result[0] if result[0] is not None else 0,
            "occhiali": result[1] if result[1] is not None else 0,
            "pneumatici": result[2] if result[2] is not None else 0
        }


    def get_annual_summary(self, year):
        total_incassi = self.get_total_incassi_by_year(year)
        total_costi = self.get_total_costi_by_year(year)
        events_by_region = self.get_events_count_by_region(year)
        top_events = self.get_top_performing_events(year)
        product_sales = self.get_product_sales_by_year(year)

        return {
            "year": year,
            "total_incassi": total_incassi,
            "total_costi": total_costi,
            "net_profit": total_incassi - total_costi,
            "events_by_region": events_by_region,
            "top_events": top_events,
            "product_sales": product_sales
        }

if __name__ == '__main__':
    # Example Usage (for testing)
    db_name = 'inventory.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create a dummy annual_events table for testing if it doesn\'t exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS annual_events (
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
    conn.commit()

    # Insert some dummy data for testing
    try:
        cursor.execute("INSERT INTO annual_events (periodo, data, nome, regione, incassi, costi_reali, caschi, occhiali, pneumatici) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       ('Q1', '2024-01-15', 'Evento Test 1', 'Lazio', 1000.0, 300.0, 50, 20, 10))
        cursor.execute("INSERT INTO annual_events (periodo, data, nome, regione, incassi, costi_reali, caschi, occhiali, pneumatici) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       ('Q2', '2024-04-20', 'Evento Test 2', 'Lombardia', 1500.0, 400.0, 70, 30, 15))
        cursor.execute("INSERT INTO annual_events (periodo, data, nome, regione, incassi, costi_reali, caschi, occhiali, pneumatici) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       ('Q3', '2024-07-10', 'Evento Test 3', 'Lazio', 800.0, 200.0, 30, 10, 5))
        conn.commit()
    except sqlite3.IntegrityError:
        print("Dummy data already exists.")

    stats = EventStatistics(conn)

    print("\n--- 2024 Annual Summary ---")
    summary_2024 = stats.get_annual_summary(2024)
    for key, value in summary_2024.items():
        print(f"{key}: {value}")

    conn.close()



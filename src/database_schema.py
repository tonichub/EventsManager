"""
Database schema for the inventory management system.
This module defines the database structure and relationships.
"""

import sqlite3
import os
import datetime

class DatabaseManager:
    def __init__(self, db_path='inventory.db'):
        """Initialize the database manager with the specified database path."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        return self.conn
        
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            
    def create_tables(self):
        """Create all necessary tables if they don't exist."""
        self.connect()
        
        # Create suppliers table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_person TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Create products table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_id INTEGER,
            sku TEXT,
            ean_upc TEXT,
            name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            subcategory TEXT,
            purchase_price REAL,
            selling_price REAL,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (supplier_id) REFERENCES suppliers (id),
            UNIQUE(ean_upc)
        )
        ''')
        
        # Create inventory table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            quantity INTEGER DEFAULT 0,
            location TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        ''')
        
        # Create inventory_transactions table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            transaction_type TEXT NOT NULL,  -- 'in' or 'out'
            quantity INTEGER NOT NULL,
            reference TEXT,  -- e.g., order number, invoice number
            notes TEXT,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user TEXT,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        ''')
        
        # Create events table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            description TEXT,
            related_id INTEGER,  -- can be product_id, transaction_id, etc.
            event_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user TEXT
        )
        ''')
        
        # Create annual_events table
        self.cursor.execute("""
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
        
        self.conn.commit()
        self.close()
        
    def initialize_database(self):
        """Initialize the database with tables and default data."""
        if not os.path.exists(self.db_path):
            self.create_tables()
            return True
        return False


if __name__ == "__main__":
    # Test database creation
    db_manager = DatabaseManager()
    db_manager.create_tables()
    print("Database schema created successfully.")

"""
Inventory management module for handling stock operations.
This module manages inventory transactions, barcode scanning, and event logging.
"""

import sqlite3
import datetime
from database_schema import DatabaseManager

class InventoryManager:
    def __init__(self, db_manager):
        """Initialize the inventory manager with a database manager."""
        self.db_manager = db_manager
        
    def get_product_by_barcode(self, barcode):
        """
        Get product details by EAN/UPC barcode.
        
        Args:
            barcode: The EAN/UPC barcode to search for
            
        Returns:
            dict: Product details or None if not found
        """
        self.db_manager.connect()
        
        self.db_manager.cursor.execute("""
            SELECT p.*, i.quantity 
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            WHERE p.ean_upc = ?
        """, (barcode,))
        
        result = self.db_manager.cursor.fetchone()
        self.db_manager.close()
        
        if result:
            return dict(result)
        return None
    
    def get_product_by_sku(self, sku):
        """
        Get product details by SKU.
        
        Args:
            sku: The SKU to search for
            
        Returns:
            dict: Product details or None if not found
        """
        self.db_manager.connect()
        
        self.db_manager.cursor.execute("""
            SELECT p.*, i.quantity 
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            WHERE p.sku = ?
        """, (sku,))
        
        result = self.db_manager.cursor.fetchone()
        self.db_manager.close()
        
        if result:
            return dict(result)
        return None
    
    def search_products(self, search_term):
        """
        Search for products by name, description, SKU, or EAN/UPC.
        
        Args:
            search_term: The search term
            
        Returns:
            list: List of matching products
        """
        self.db_manager.connect()
        
        search_pattern = f"%{search_term}%"
        self.db_manager.cursor.execute("""
            SELECT p.*, i.quantity 
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            WHERE p.name LIKE ? 
            OR p.description LIKE ? 
            OR p.sku LIKE ? 
            OR p.ean_upc LIKE ?
        """, (search_pattern, search_pattern, search_pattern, search_pattern))
        
        results = self.db_manager.cursor.fetchall()
        self.db_manager.close()
        
        return [dict(row) for row in results]
    
    def add_stock(self, product_id, quantity, reference=None, notes=None, user=None):
        """
        Add stock to inventory (stock in).
        
        Args:
            product_id: The product ID
            quantity: Quantity to add
            reference: Reference document (e.g., purchase order)
            notes: Additional notes
            user: User performing the operation
            
        Returns:
            bool: True if successful, False otherwise
        """
        if quantity <= 0:
            return False
            
        try:
            self.db_manager.connect()
            
            # Update inventory
            self.db_manager.cursor.execute("""
                UPDATE inventory 
                SET quantity = quantity + ?, last_updated = CURRENT_TIMESTAMP
                WHERE product_id = ?
            """, (quantity, product_id))
            
            # Record transaction
            self.db_manager.cursor.execute("""
                INSERT INTO inventory_transactions 
                (product_id, transaction_type, quantity, reference, notes, user)
                VALUES (?, 'in', ?, ?, ?, ?)
            """, (product_id, quantity, reference, notes, user))
            
            transaction_id = self.db_manager.cursor.lastrowid
            
            # Log event
            self.db_manager.cursor.execute("""
                INSERT INTO annual_events 
                (event_type, description, related_id, user)
                VALUES (?, ?, ?, ?)
            """, (
                'stock_in',
                f'Added {quantity} units to stock',
                transaction_id,
                user
            ))
            
            self.db_manager.conn.commit()
            self.db_manager.close()
            return True
            
        except Exception as e:
            if self.db_manager.conn:
                self.db_manager.conn.rollback()
                self.db_manager.close()
            print(f"Error adding stock: {str(e)}")
            return False
    
    def remove_stock(self, product_id, quantity, reference=None, notes=None, user=None):
        """
        Remove stock from inventory (stock out).
        
        Args:
            product_id: The product ID
            quantity: Quantity to remove
            reference: Reference document (e.g., sales order)
            notes: Additional notes
            user: User performing the operation
            
        Returns:
            bool: True if successful, False otherwise
        """
        if quantity <= 0:
            return False
            
        try:
            self.db_manager.connect()
            
            # Check current quantity
            self.db_manager.cursor.execute("""
                SELECT quantity FROM inventory WHERE product_id = ?
            """, (product_id,))
            
            result = self.db_manager.cursor.fetchone()
            if not result or result['quantity'] < quantity:
                self.db_manager.close()
                return False  # Not enough stock
            
            # Update inventory
            self.db_manager.cursor.execute("""
                UPDATE inventory 
                SET quantity = quantity - ?, last_updated = CURRENT_TIMESTAMP
                WHERE product_id = ?
            """, (quantity, product_id))
            
            # Record transaction
            self.db_manager.cursor.execute("""
                INSERT INTO inventory_transactions 
                (product_id, transaction_type, quantity, reference, notes, user)
                VALUES (?, 'out', ?, ?, ?, ?)
            """, (product_id, quantity, reference, notes, user))
            
            transaction_id = self.db_manager.cursor.lastrowid
            
            # Log event
            self.db_manager.cursor.execute("""
                INSERT INTO annual_events 
                (event_type, description, related_id, user)
                VALUES (?, ?, ?, ?)
            """, (
                'stock_out',
                f'Removed {quantity} units from stock',
                transaction_id,
                user
            ))
            
            self.db_manager.conn.commit()
            self.db_manager.close()
            return True
            
        except Exception as e:
            if self.db_manager.conn:
                self.db_manager.conn.rollback()
                self.db_manager.close()
            print(f"Error removing stock: {str(e)}")
            return False
    
    def get_inventory_status(self, limit=100, offset=0):
        """
        Get current inventory status.
        
        Args:
            limit: Maximum number of records to return
            offset: Offset for pagination
            
        Returns:
            list: List of inventory items with product details
        """
        self.db_manager.connect()
        
        self.db_manager.cursor.execute("""
            SELECT p.id, p.sku, p.ean_upc, p.name, p.description, 
                   p.purchase_price, p.selling_price, i.quantity, i.last_updated,
                   s.name as supplier_name
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            LEFT JOIN suppliers s ON p.supplier_id = s.id
            ORDER BY p.name
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        results = self.db_manager.cursor.fetchall()
        self.db_manager.close()
        
        return [dict(row) for row in results]
    
    def get_low_stock_items(self, threshold=5):
        """
        Get items with stock below the specified threshold.
        
        Args:
            threshold: Stock threshold
            
        Returns:
            list: List of low stock items
        """
        self.db_manager.connect()
        
        self.db_manager.cursor.execute("""
            SELECT p.id, p.sku, p.ean_upc, p.name, p.description, 
                   p.purchase_price, p.selling_price, i.quantity, i.last_updated,
                   s.name as supplier_name
            FROM products p
            JOIN inventory i ON p.id = i.product_id
            LEFT JOIN suppliers s ON p.supplier_id = s.id
            WHERE i.quantity <= ?
            ORDER BY i.quantity ASC
        """, (threshold,))
        
        results = self.db_manager.cursor.fetchall()
        self.db_manager.close()
        
        return [dict(row) for row in results]
    
    def get_transaction_history(self, product_id=None, start_date=None, end_date=None, limit=100):
        """
        Get transaction history for a product or all products.
        
        Args:
            product_id: Product ID (optional)
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            limit: Maximum number of records to return
            
        Returns:
            list: List of transactions
        """
        self.db_manager.connect()
        
        query = """
            SELECT t.*, p.name as product_name, p.sku, p.ean_upc
            FROM inventory_transactions t
            JOIN products p ON t.product_id = p.id
        """
        
        conditions = []
        params = []
        
        if product_id:
            conditions.append("t.product_id = ?")
            params.append(product_id)
            
        if start_date:
            conditions.append("t.transaction_date >= ?")
            params.append(start_date)
            
        if end_date:
            conditions.append("t.transaction_date <= ?")
            params.append(end_date)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY t.transaction_date DESC LIMIT ?"
        params.append(limit)
        
        self.db_manager.cursor.execute(query, params)
        results = self.db_manager.cursor.fetchall()
        self.db_manager.close()
        
        return [dict(row) for row in results]
    def get_events(self, event_type=None, start_date=None, end_date=None, limit=100):
        """
        Get events from the system.
        
        Args:
            event_type: Type of event to filter (optional)
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            limit: Maximum number of records to return
            
        Returns:
            list: List of events
        """
        self.db_manager.connect()
        
        query = "SELECT * FROM annual_events"
        
        conditions = []
        params = []
        
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type)
            
        if start_date:
            conditions.append("event_date >= ?")
            params.append(start_date)
            
        if end_date:
            conditions.append("event_date <= ?")
            params.append(end_date)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY event_date DESC LIMIT ?"
        params.append(limit)
        
        self.db_manager.cursor.execute(query, params)
        results = self.db_manager.cursor.fetchall()
        self.db_manager.close()
        
        return [dict(row) for row in results]


if __name__ == "__main__":
    # Test the inventory manager
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    inventory_manager = InventoryManager(db_manager)
    
    # Test inventory operations
    print("Inventory status:", inventory_manager.get_inventory_status(limit=5))

#!/usr/bin/env python3
"""
Product Importer Module - Handles importing products from inventory to events
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple

class EventProductImporter:
    """
    Handles the import of products from inventory to events.
    Provides functionality to select products, assign quantities, and manage availability.
    """
    
    def __init__(self, db_manager, inventory_manager, event_manager):
        """
        Initialize the Event Product Importer.
        
        Args:
            db_manager: Database manager instance
            inventory_manager: Inventory manager instance
            event_manager: Event manager instance
        """
        self.db_manager = db_manager
        self.inventory_manager = inventory_manager
        self.event_manager = event_manager
    
    def get_available_products(self, category: Optional[str] = None, 
                              supplier: Optional[str] = None,
                              min_stock: int = 1) -> pd.DataFrame:
        """
        Get available products that can be assigned to events.
        
        Args:
            category: Optional category filter
            supplier: Optional supplier filter
            min_stock: Minimum available stock required
            
        Returns:
            pd.DataFrame: Available products
        """
        query_parts = [
            "SELECT p.id, p.sku, p.name, p.description, p.category, s.name as supplier, p.purchase_price, p.sale_price, i.quantity",
            "FROM products p",
            "JOIN inventory i ON p.id = i.product_id",
            "JOIN suppliers s ON p.supplier_id = s.id",
            "WHERE i.quantity >= ?"
        ]
        
        params = [min_stock]
        
        if category:
            query_parts.append("AND p.category = ?")
            params.append(category)
        
        if supplier:
            query_parts.append("AND s.name = ?")
            params.append(supplier)
        
        query_parts.append("ORDER BY p.category, p.name")
        
        query = " ".join(query_parts)
        results = self.db_manager.execute_query(query, tuple(params))
        
        if not results:
            return pd.DataFrame()
        
        return pd.DataFrame(results, columns=[
            'ID', 'SKU', 'Name', 'Description', 'Category', 
            'Supplier', 'Purchase Price', 'Sale Price', 'Available Stock'
        ])
    
    def import_products_to_event(self, event_id: int, product_selections: List[Dict[str, Any]]) -> bool:
        """
        Import selected products to an event.
        
        Args:
            event_id: Event ID
            product_selections: List of dictionaries with product selection details
                Each dict should contain:
                - product_id: Product ID
                - quantity: Quantity to assign
                - sale_price: Optional override of sale price for this event
            
        Returns:
            bool: Success status
        """
        # Begin transaction
        self.db_manager.begin_transaction()
        
        try:
            for selection in product_selections:
                product_id = selection['product_id']
                quantity = selection['quantity']
                
                # Check if product has enough stock
                available_stock = self.inventory_manager.get_available_stock(product_id)
                if available_stock < quantity:
                    raise ValueError(f"Not enough stock for product ID {product_id}. Available: {available_stock}, Requested: {quantity}")
                
                # Check if product is already assigned to this event
                query = "SELECT id FROM event_products WHERE event_id = ? AND product_id = ?"
                existing = self.db_manager.execute_query(query, (event_id, product_id))
                
                if existing:
                    # Update existing assignment
                    query = """
                    UPDATE event_products 
                    SET quantity_assigned = quantity_assigned + ?, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE event_id = ? AND product_id = ?
                    """
                    self.db_manager.execute_query(query, (quantity, event_id, product_id))
                else:
                    # Create new assignment
                    query = """
                    INSERT INTO event_products (event_id, product_id, quantity_assigned, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """
                    self.db_manager.execute_query(query, (event_id, product_id, quantity))
                
                # Reserve stock in inventory
                self.inventory_manager.reserve_stock(product_id, quantity, f"Reserved for event #{event_id}")
                
                # If custom sale price is provided, store it
                if 'sale_price' in selection and selection['sale_price'] is not None:
                    query = """
                    UPDATE event_products 
                    SET event_sale_price = ?
                    WHERE event_id = ? AND product_id = ?
                    """
                    self.db_manager.execute_query(query, (selection['sale_price'], event_id, product_id))
            
            # Commit transaction
            self.db_manager.commit_transaction()
            return True
            
        except Exception as e:
            # Rollback transaction on error
            self.db_manager.rollback_transaction()
            raise e
    
    def get_event_products(self, event_id: int) -> pd.DataFrame:
        """
        Get products assigned to a specific event.
        
        Args:
            event_id: Event ID
            
        Returns:
            pd.DataFrame: Products assigned to the event
        """
        query = """
        SELECT 
            p.id, 
            p.sku, 
            p.name, 
            p.description, 
            p.category,
            s.name as supplier,
            p.purchase_price,
            COALESCE(ep.event_sale_price, p.sale_price) as sale_price,
            ep.quantity_assigned,
            i.quantity as available_stock
        FROM 
            event_products ep
        JOIN 
            products p ON ep.product_id = p.id
        JOIN 
            suppliers s ON p.supplier_id = s.id
        JOIN 
            inventory i ON p.id = i.product_id
        WHERE 
            ep.event_id = ?
        ORDER BY 
            p.category, p.name
        """
        
        results = self.db_manager.execute_query(query, (event_id,))
        
        if not results:
            return pd.DataFrame()
        
        return pd.DataFrame(results, columns=[
            'ID', 'SKU', 'Name', 'Description', 'Category', 
            'Supplier', 'Purchase Price', 'Sale Price', 'Assigned Quantity', 'Available Stock'
        ])
    
    def update_event_product_quantity(self, event_id: int, product_id: int, new_quantity: int) -> bool:
        """
        Update the quantity of a product assigned to an event.
        
        Args:
            event_id: Event ID
            product_id: Product ID
            new_quantity: New quantity to assign
            
        Returns:
            bool: Success status
        """
        # Begin transaction
        self.db_manager.begin_transaction()
        
        try:
            # Get current assigned quantity
            query = "SELECT quantity_assigned FROM event_products WHERE event_id = ? AND product_id = ?"
            result = self.db_manager.execute_query(query, (event_id, product_id))
            
            if not result:
                raise ValueError(f"Product ID {product_id} is not assigned to event ID {event_id}")
            
            current_quantity = result[0][0]
            quantity_difference = new_quantity - current_quantity
            
            if quantity_difference > 0:
                # Need to reserve more stock
                available_stock = self.inventory_manager.get_available_stock(product_id)
                if available_stock < quantity_difference:
                    raise ValueError(f"Not enough stock for product ID {product_id}. Available: {available_stock}, Additional needed: {quantity_difference}")
                
                # Reserve additional stock
                self.inventory_manager.reserve_stock(product_id, quantity_difference, f"Additional reservation for event #{event_id}")
            
            elif quantity_difference < 0:
                # Releasing some stock
                self.inventory_manager.release_reserved_stock(product_id, abs(quantity_difference), f"Released from event #{event_id}")
            
            # Update assignment
            query = """
            UPDATE event_products 
            SET quantity_assigned = ?, 
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND product_id = ?
            """
            self.db_manager.execute_query(query, (new_quantity, event_id, product_id))
            
            # Commit transaction
            self.db_manager.commit_transaction()
            return True
            
        except Exception as e:
            # Rollback transaction on error
            self.db_manager.rollback_transaction()
            raise e
    
    def remove_product_from_event(self, event_id: int, product_id: int) -> bool:
        """
        Remove a product from an event and release reserved stock.
        
        Args:
            event_id: Event ID
            product_id: Product ID
            
        Returns:
            bool: Success status
        """
        # Begin transaction
        self.db_manager.begin_transaction()
        
        try:
            # Get current assigned quantity
            query = "SELECT quantity_assigned FROM event_products WHERE event_id = ? AND product_id = ?"
            result = self.db_manager.execute_query(query, (event_id, product_id))
            
            if not result:
                raise ValueError(f"Product ID {product_id} is not assigned to event ID {event_id}")
            
            current_quantity = result[0][0]
            
            # Release reserved stock
            self.inventory_manager.release_reserved_stock(product_id, current_quantity, f"Released from event #{event_id}")
            
            # Remove assignment
            query = "DELETE FROM event_products WHERE event_id = ? AND product_id = ?"
            self.db_manager.execute_query(query, (event_id, product_id))
            
            # Commit transaction
            self.db_manager.commit_transaction()
            return True
            
        except Exception as e:
            # Rollback transaction on error
            self.db_manager.rollback_transaction()
            raise e
    
    def import_products_from_excel(self, event_id: int, excel_file: str) -> Tuple[int, List[str]]:
        """
        Import products to an event from an Excel file.
        
        Args:
            event_id: Event ID
            excel_file: Path to Excel file
            
        Returns:
            Tuple[int, List[str]]: Number of products imported and list of errors
        """
        try:
            # Read Excel file
            df = pd.read_excel(excel_file)
            
            # Validate required columns
            required_columns = ['SKU', 'Quantity']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return 0, [f"Missing required columns: {', '.join(missing_columns)}"]
            
            # Process each row
            imported_count = 0
            errors = []
            
            for _, row in df.iterrows():
                try:
                    sku = row['SKU']
                    quantity = int(row['Quantity'])
                    
                    # Get product ID from SKU
                    query = "SELECT id FROM products WHERE sku = ?"
                    result = self.db_manager.execute_query(query, (sku,))
                    
                    if not result:
                        errors.append(f"Product with SKU '{sku}' not found")
                        continue
                    
                    product_id = result[0][0]
                    
                    # Prepare product selection
                    selection = {
                        'product_id': product_id,
                        'quantity': quantity
                    }
                    
                    # Add custom sale price if provided
                    if 'Sale Price' in df.columns and not pd.isna(row['Sale Price']):
                        selection['sale_price'] = float(row['Sale Price'])
                    
                    # Import product
                    self.import_products_to_event(event_id, [selection])
                    imported_count += 1
                    
                except Exception as e:
                    errors.append(f"Error importing SKU '{row['SKU']}': {str(e)}")
            
            return imported_count, errors
            
        except Exception as e:
            return 0, [f"Error reading Excel file: {str(e)}"]

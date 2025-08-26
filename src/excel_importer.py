"""
Excel importer module for the inventory management system.
This module handles importing data from various Excel file formats.
"""

import pandas as pd
import os
import re
from database_schema import DatabaseManager

class ExcelImporter:
    def __init__(self, db_manager):
        """Initialize the Excel importer with a database manager."""
        self.db_manager = db_manager
        
    def detect_column_mapping(self, df):
        """
        Automatically detect and map columns from Excel to database fields.
        Returns a dictionary mapping standard field names to Excel column names.
        """
        mapping = {
            'ean_upc': None,
            'sku': None,
            'name': None,
            'description': None,
            'purchase_price': None,
            'selling_price': None,
            'status': None,
            'category': None,
            'supplier': None
        }
        
        # Convert all column names to lowercase for case-insensitive matching
        columns_lower = {col.lower(): col for col in df.columns}
        
        # Define patterns for each field
        patterns = {
            'ean_upc': ['ean', 'upc', 'barcode', 'ean code', 'upc code'],
            'sku': ['sku', 'article', 'article #', 'item', 'item code', 'product code'],
            'name': ['name', 'model name', 'product name', 'model'],
            'description': ['description', 'product description', 'desc'],
            'purchase_price': ['dealer price', 'purchase', 'cost', 'whs', 'whs price', 'wholesale'],
            'selling_price': ['retail', 'price', 'rrp', 'suggested retail', 'selling price'],
            'status': ['status'],
            'category': ['category', 'line', 'product line', 'type'],
            'supplier': ['supplier', 'vendor', 'brand']
        }
        
        # Try to match columns based on patterns
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                for col_lower, original_col in columns_lower.items():
                    if pattern in col_lower:
                        mapping[field] = original_col
                        break
                if mapping[field]:
                    break
        
        return mapping
    
    def clean_data(self, df, mapping):
        """Clean and prepare data for import."""
        # Create a new DataFrame with only the mapped columns
        cleaned_df = pd.DataFrame()
        
        for field, col in mapping.items():
            if col is not None and col in df.columns:
                cleaned_df[field] = df[col]
        
        # Remove rows where essential fields are missing
        if 'name' in cleaned_df.columns:
            cleaned_df = cleaned_df.dropna(subset=['name'])
        
        # Convert price columns to numeric
        for price_field in ['purchase_price', 'selling_price']:
            if price_field in cleaned_df.columns:
                cleaned_df[price_field] = pd.to_numeric(cleaned_df[price_field], errors='coerce')
        
        return cleaned_df
    
    def import_excel_file(self, file_path, supplier_name=None, skip_rows=0):
        """
        Import data from an Excel file into the database.
        
        Args:
            file_path: Path to the Excel file
            supplier_name: Name of the supplier (optional)
            skip_rows: Number of header rows to skip
            
        Returns:
            dict: Import statistics
        """
        try:
            # Determine the engine based on file extension
            if file_path.endswith('.xls'):
                df = pd.read_excel(file_path, engine='xlrd', skiprows=skip_rows)
            else:
                df = pd.read_excel(file_path, skiprows=skip_rows)
            
            # Drop completely empty rows and columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # Detect column mapping
            mapping = self.detect_column_mapping(df)
            
            # Clean data
            cleaned_df = self.clean_data(df, mapping)
            
            # Connect to database
            self.db_manager.connect()
            
            # Get or create supplier
            supplier_id = None
            if supplier_name:
                self.db_manager.cursor.execute(
                    "SELECT id FROM suppliers WHERE name = ?", 
                    (supplier_name,)
                )
                result = self.db_manager.cursor.fetchone()
                
                if result:
                    supplier_id = result['id']
                else:
                    self.db_manager.cursor.execute(
                        "INSERT INTO suppliers (name) VALUES (?)",
                        (supplier_name,)
                    )
                    supplier_id = self.db_manager.cursor.lastrowid
            
            # Import products
            products_added = 0
            products_updated = 0
            
            for _, row in cleaned_df.iterrows():
                # Skip rows that don't have at least a name
                if 'name' not in row or pd.isna(row['name']):
                    continue
                
                # Prepare product data
                product_data = {
                    'supplier_id': supplier_id,
                    'name': row.get('name', ''),
                    'sku': row.get('sku', None),
                    'ean_upc': row.get('ean_upc', None),
                    'description': row.get('description', None),
                    'purchase_price': row.get('purchase_price', None),
                    'selling_price': row.get('selling_price', None),
                    'status': row.get('status', None),
                    'category': row.get('category', None)
                }
                
                # Remove None values
                product_data = {k: v for k, v in product_data.items() if v is not None}
                
                # Check if product exists (by EAN/UPC or SKU)
                product_id = None
                if 'ean_upc' in product_data and product_data['ean_upc']:
                    self.db_manager.cursor.execute(
                        "SELECT id FROM products WHERE ean_upc = ?", 
                        (product_data['ean_upc'],)
                    )
                    result = self.db_manager.cursor.fetchone()
                    if result:
                        product_id = result['id']
                
                if not product_id and 'sku' in product_data and product_data['sku']:
                    self.db_manager.cursor.execute(
                        "SELECT id FROM products WHERE sku = ?", 
                        (product_data['sku'],)
                    )
                    result = self.db_manager.cursor.fetchone()
                    if result:
                        product_id = result['id']
                
                # Update or insert product
                if product_id:
                    # Update existing product
                    set_clause = ", ".join([f"{k} = ?" for k in product_data.keys()])
                    query = f"UPDATE products SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
                    self.db_manager.cursor.execute(query, list(product_data.values()) + [product_id])
                    products_updated += 1
                else:
                    # Insert new product
                    columns = ", ".join(product_data.keys())
                    placeholders = ", ".join(["?"] * len(product_data))
                    query = f"INSERT INTO products ({columns}) VALUES ({placeholders})"
                    self.db_manager.cursor.execute(query, list(product_data.values()))
                    product_id = self.db_manager.cursor.lastrowid
                    products_added += 1
                    
                    # Initialize inventory record for new product
                    self.db_manager.cursor.execute(
                        "INSERT INTO inventory (product_id, quantity) VALUES (?, 0)",
                        (product_id,)
                    )
            
            # Commit changes
            self.db_manager.conn.commit()
            
            # Close connection
            self.db_manager.close()
            
            return {
                'file': os.path.basename(file_path),
                'products_added': products_added,
                'products_updated': products_updated,
                'column_mapping': mapping
            }
            
        except Exception as e:
            if self.db_manager.conn:
                self.db_manager.conn.rollback()
                self.db_manager.close()
            raise e

    def detect_header_row(self, file_path):
        """
        Detect the header row in an Excel file by looking for key columns.
        Returns the index of the likely header row.
        """
        try:
            # Read the first 20 rows to analyze
            if file_path.endswith('.xls'):
                df = pd.read_excel(file_path, engine='xlrd', nrows=20, header=None)
            else:
                df = pd.read_excel(file_path, nrows=20, header=None)
            
            # Keywords that might indicate a header row
            header_keywords = ['sku', 'ean', 'upc', 'code', 'article', 'name', 'description', 
                              'price', 'retail', 'dealer', 'product']
            
            # Check each row for header keywords
            for i in range(len(df)):
                row = df.iloc[i].astype(str).str.lower()
                matches = sum(row.str.contains('|'.join(header_keywords), regex=True))
                if matches >= 3:  # If at least 3 keywords are found, consider it a header row
                    return i
            
            # Default to first row if no clear header is found
            return 0
            
        except Exception as e:
            print(f"Error detecting header row: {str(e)}")
            return 0
    
    def guess_supplier_from_file(self, file_path):
        """
        Try to guess the supplier name from the file name or content.
        """
        file_name = os.path.basename(file_path).lower()
        
        # Check file name for known supplier names
        supplier_patterns = {
            'bolle': 'Bollé',
            'ffwd': 'FFWD',
            'bis': 'B.I.S. Srl',
            'premium': 'Premium'
        }
        
        for pattern, supplier in supplier_patterns.items():
            if pattern in file_name:
                return supplier
        
        # If not found in filename, try to read first few rows
        try:
            if file_path.endswith('.xls'):
                df = pd.read_excel(file_path, engine='xlrd', nrows=10)
            else:
                df = pd.read_excel(file_path, nrows=10)
                
            # Convert to string and search for supplier names
            df_str = df.astype(str).values.flatten()
            for text in df_str:
                if pd.notna(text):
                    for pattern, supplier in supplier_patterns.items():
                        if pattern.lower() in str(text).lower():
                            return supplier
        except:
            pass
            
        # Return a default name based on the file name if no match
        return os.path.splitext(file_name)[0]


if __name__ == "__main__":
    # Test the importer
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    importer = ExcelImporter(db_manager)
    
    # Test file path
    test_file = "/path/to/test/file.xlsx"
    
    if os.path.exists(test_file):
        header_row = importer.detect_header_row(test_file)
        supplier = importer.guess_supplier_from_file(test_file)
        result = importer.import_excel_file(test_file, supplier, header_row)
        print(f"Import result: {result}")
    else:
        print("Test file not found.")

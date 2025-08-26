"""
Barcode scanner module for the inventory management system.
This module handles barcode scanning functionality.
"""

import re
import time

class BarcodeScanner:
    def __init__(self, inventory_manager):
        """Initialize the barcode scanner with an inventory manager."""
        self.inventory_manager = inventory_manager
        self.last_scan = None
        self.scan_timeout = 2.0  # seconds
        
    def scan_barcode(self, barcode):
        """
        Process a scanned barcode and return product information.
        
        Args:
            barcode: The scanned barcode (EAN/UPC)
            
        Returns:
            dict: Product details or None if not found
        """
        # Clean the barcode input
        barcode = self._clean_barcode(barcode)
        
        # Validate barcode format
        if not self._validate_barcode(barcode):
            return {'error': 'Invalid barcode format'}
        
        # Check for duplicate scan (same barcode within timeout period)
        current_time = time.time()
        if self.last_scan and self.last_scan['barcode'] == barcode:
            if current_time - self.last_scan['timestamp'] < self.scan_timeout:
                return {'error': 'Duplicate scan', 'product': self.last_scan['product']}
        
        # Look up product by barcode
        product = self.inventory_manager.get_product_by_barcode(barcode)
        
        # Update last scan information
        self.last_scan = {
            'barcode': barcode,
            'timestamp': current_time,
            'product': product
        }
        
        return product
    
    def _clean_barcode(self, barcode):
        """Clean barcode input by removing non-numeric characters."""
        return re.sub(r'[^0-9]', '', barcode)
    
    def _validate_barcode(self, barcode):
        """
        Validate barcode format (EAN/UPC).
        
        Args:
            barcode: The barcode to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Basic validation: check if it's a numeric string of appropriate length
        if not barcode.isdigit():
            return False
        
        # Common barcode lengths
        valid_lengths = [8, 12, 13, 14]
        if len(barcode) not in valid_lengths:
            return False
            
        # For EAN/UPC, validate check digit
        if len(barcode) in [8, 12, 13]:
            return self._validate_check_digit(barcode)
            
        return True
    
    def _validate_check_digit(self, barcode):
        """
        Validate the check digit of an EAN/UPC barcode.
        
        Args:
            barcode: The barcode to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Extract the check digit (last digit)
        check_digit = int(barcode[-1])
        
        # Calculate the expected check digit
        digits = [int(d) for d in barcode[:-1]]
        odd_sum = sum(digits[::2])
        even_sum = sum(digits[1::2])
        
        if len(barcode) in [8, 12]:  # UPC-E, UPC-A
            total = odd_sum * 3 + even_sum
        else:  # EAN-13
            total = even_sum * 3 + odd_sum
            
        expected_check_digit = (10 - (total % 10)) % 10
        
        return check_digit == expected_check_digit
    
    def process_batch_scan(self, barcodes):
        """
        Process a batch of scanned barcodes.
        
        Args:
            barcodes: List of barcodes
            
        Returns:
            dict: Results of batch scan
        """
        results = {
            'total': len(barcodes),
            'found': 0,
            'not_found': 0,
            'invalid': 0,
            'products': []
        }
        
        for barcode in barcodes:
            clean_barcode = self._clean_barcode(barcode)
            
            if not self._validate_barcode(clean_barcode):
                results['invalid'] += 1
                continue
                
            product = self.inventory_manager.get_product_by_barcode(clean_barcode)
            
            if product:
                results['found'] += 1
                results['products'].append(product)
            else:
                results['not_found'] += 1
                
        return results


if __name__ == "__main__":
    # Test the barcode scanner
    from database_schema import DatabaseManager
    from inventory_manager import InventoryManager
    
    db_manager = DatabaseManager()
    inventory_manager = InventoryManager(db_manager)
    scanner = BarcodeScanner(inventory_manager)
    
    # Test barcode validation
    test_barcodes = [
        '5901234123457',  # Valid EAN-13
        '12345678',       # Valid EAN-8
        '123456',         # Invalid (too short)
        'ABC123456789'    # Invalid (non-numeric)
    ]
    
    for barcode in test_barcodes:
        clean = scanner._clean_barcode(barcode)
        valid = scanner._validate_barcode(clean)
        print(f"Barcode: {barcode}, Clean: {clean}, Valid: {valid}")

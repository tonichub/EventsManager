#!/usr/bin/env python3
"""
Event Validator Module - Tests and validates the event management workflow
"""

import os
import sys
import unittest
import tempfile
import pandas as pd
import sqlite3
import datetime
from typing import List, Dict, Any, Optional, Tuple

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from event_manager.event_manager import EventManager
from event_manager.product_importer import ProductImporter
from event_manager.excel_generator import ExcelGenerator
from event_manager.event_statistics import EventStatistics
from database_schema import DatabaseManager


class EventWorkflowValidator:
    """
    Validates the complete event management workflow.
    Tests integration between modules and ensures correct functionality.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize the validator with a test database.
        
        Args:
            db_path: Path to the database file (uses in-memory DB if None)
        """
        # Use in-memory database if no path provided
        self.db_path = db_path or ':memory:'
        self.db_manager = DatabaseManager(self.db_path)
        
        # Initialize modules
        self.event_manager = EventManager(self.db_manager)
        self.product_importer = ProductImporter(self.db_manager)
        self.excel_generator = ExcelGenerator(self.db_manager)
        self.event_statistics = EventStatistics(self.db_manager, self.event_manager)
        
        # Create temporary directory for output files
        self.temp_dir = tempfile.mkdtemp()
        
        # Setup test data
        self._setup_test_data()
    
    def _setup_test_data(self):
        """Set up test data for validation"""
        # Create tables if they don't exist
        self.db_manager.create_tables()
        
        # Add test suppliers
        suppliers = [
            ('Bollé', 'France', 'contact@bolle.com', '+33123456789'),
            ('FFWD', 'Netherlands', 'info@ffwd.com', '+31987654321'),
            ('B.I.S. Srl', 'Italy', 'info@bis.it', '+39123456789')
        ]
        
        for supplier in suppliers:
            self.db_manager.execute_query(
                "INSERT OR IGNORE INTO suppliers (name, country, email, phone) VALUES (?, ?, ?, ?)",
                supplier
            )
        
        # Add test products
        products = [
            ('SKU001', 'Bollé Helmet X1', 'Premium cycling helmet', 'Helmets', 1, 80.0, 150.0, '1234567890123'),
            ('SKU002', 'Bollé Sunglasses Pro', 'Sports sunglasses', 'Eyewear', 1, 60.0, 120.0, '1234567890124'),
            ('SKU003', 'FFWD Wheels F4R', 'Carbon wheels', 'Wheels', 2, 800.0, 1200.0, '1234567890125'),
            ('SKU004', 'FFWD Handlebar', 'Carbon handlebar', 'Components', 2, 200.0, 350.0, '1234567890126'),
            ('SKU005', 'BIS Premium Saddle', 'Leather saddle', 'Components', 3, 90.0, 180.0, '1234567890127')
        ]
        
        for product in products:
            self.db_manager.execute_query(
                """
                INSERT OR IGNORE INTO products 
                (sku, name, description, category, supplier_id, purchase_price, sale_price, ean_code) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                product
            )
        
        # Add test inventory
        inventory_items = [
            (1, 10, 'warehouse'),
            (2, 15, 'warehouse'),
            (3, 5, 'warehouse'),
            (4, 8, 'warehouse'),
            (5, 12, 'warehouse')
        ]
        
        for item in inventory_items:
            self.db_manager.execute_query(
                "INSERT OR IGNORE INTO inventory (product_id, quantity, location) VALUES (?, ?, ?)",
                item
            )
        
        # Add test events
        events = [
            ('Bike Expo 2025', '2025-03-15', '2025-03-17', 'Milan', 'Major cycling exhibition'),
            ('Summer Sports Fair', '2025-06-20', '2025-06-22', 'Rome', 'Summer sports equipment fair'),
            ('Mountain Bike Festival', '2025-09-05', '2025-09-07', 'Alps', 'Mountain biking event')
        ]
        
        for event in events:
            self.db_manager.execute_query(
                """
                INSERT OR IGNORE INTO events 
                (name, start_date, end_date, location, description) 
                VALUES (?, ?, ?, ?, ?)
                """,
                event
            )
    
    def validate_complete_workflow(self) -> Dict[str, Any]:
        """
        Validate the complete event management workflow.
        
        Returns:
            Dict[str, Any]: Validation results with status and messages
        """
        results = {
            'overall_status': 'PASSED',
            'steps': [],
            'messages': []
        }
        
        try:
            # Step 1: Test event creation
            event_id = self._test_event_creation()
            results['steps'].append({
                'name': 'Event Creation',
                'status': 'PASSED',
                'message': f'Successfully created event with ID {event_id}'
            })
            
            # Step 2: Test product import
            product_count = self._test_product_import(event_id)
            results['steps'].append({
                'name': 'Product Import',
                'status': 'PASSED',
                'message': f'Successfully imported {product_count} products to event'
            })
            
            # Step 3: Test sales recording
            sales_count = self._test_sales_recording(event_id)
            results['steps'].append({
                'name': 'Sales Recording',
                'status': 'PASSED',
                'message': f'Successfully recorded {sales_count} sales transactions'
            })
            
            # Step 4: Test Excel generation
            excel_path = self._test_excel_generation(event_id)
            results['steps'].append({
                'name': 'Excel Generation',
                'status': 'PASSED',
                'message': f'Successfully generated Excel file at {excel_path}'
            })
            
            # Step 5: Test statistics generation
            stats_path = self._test_statistics_generation(event_id)
            results['steps'].append({
                'name': 'Statistics Generation',
                'status': 'PASSED',
                'message': f'Successfully generated statistics report at {stats_path}'
            })
            
            # Step 6: Test annual report generation
            annual_path = self._test_annual_report()
            results['steps'].append({
                'name': 'Annual Report',
                'status': 'PASSED',
                'message': f'Successfully generated annual report at {annual_path}'
            })
            
            results['messages'].append('All validation steps completed successfully')
            
        except Exception as e:
            results['overall_status'] = 'FAILED'
            results['messages'].append(f'Validation failed: {str(e)}')
            
            # Add failed step
            if len(results['steps']) < 6:
                step_names = ['Event Creation', 'Product Import', 'Sales Recording', 
                             'Excel Generation', 'Statistics Generation', 'Annual Report']
                
                results['steps'].append({
                    'name': step_names[len(results['steps'])],
                    'status': 'FAILED',
                    'message': str(e)
                })
        
        return results
    
    def _test_event_creation(self) -> int:
        """Test event creation functionality"""
        event_data = {
            'name': 'Test Event 2025',
            'start_date': '2025-10-15',
            'end_date': '2025-10-17',
            'location': 'Test Location',
            'description': 'Test event for validation'
        }
        
        event_id = self.event_manager.create_event(
            event_data['name'],
            event_data['start_date'],
            event_data['end_date'],
            event_data['location'],
            event_data['description']
        )
        
        if not event_id:
            raise ValueError("Failed to create event")
        
        # Verify event was created
        query = "SELECT * FROM events WHERE id = ?"
        event = self.db_manager.execute_query(query, (event_id,))
        
        if not event:
            raise ValueError(f"Event with ID {event_id} not found in database")
        
        return event_id
    
    def _test_product_import(self, event_id: int) -> int:
        """Test product import functionality"""
        # Get all products
        query = "SELECT id FROM products"
        products = self.db_manager.execute_query(query)
        
        if not products:
            raise ValueError("No products found in database")
        
        product_ids = [p[0] for p in products]
        
        # Import products to event
        for product_id in product_ids:
            # Assign random quantity between 1 and 5
            import random
            quantity = random.randint(1, 5)
            
            # Get product details
            query = "SELECT sale_price FROM products WHERE id = ?"
            product = self.db_manager.execute_query(query, (product_id,))
            
            if not product:
                raise ValueError(f"Product with ID {product_id} not found")
            
            sale_price = product[0][0]
            
            # Add product to event
            self.product_importer.assign_product_to_event(
                event_id,
                product_id,
                quantity,
                sale_price
            )
        
        # Verify products were imported
        query = "SELECT COUNT(*) FROM event_products WHERE event_id = ?"
        count = self.db_manager.execute_query(query, (event_id,))
        
        if not count or count[0][0] == 0:
            raise ValueError(f"No products assigned to event {event_id}")
        
        return count[0][0]
    
    def _test_sales_recording(self, event_id: int) -> int:
        """Test sales recording functionality"""
        # Get products assigned to event
        query = "SELECT product_id, quantity, sale_price FROM event_products WHERE event_id = ?"
        event_products = self.db_manager.execute_query(query, (event_id,))
        
        if not event_products:
            raise ValueError(f"No products assigned to event {event_id}")
        
        # Record sales for each product
        sales_count = 0
        for product in event_products:
            product_id = product[0]
            available_qty = product[1]
            sale_price = product[2]
            
            # Sell between 1 and available quantity
            import random
            sale_qty = random.randint(1, available_qty)
            
            # Record sale
            sale_date = datetime.date.today().isoformat()
            self.event_manager.record_sale(
                event_id,
                product_id,
                sale_qty,
                sale_price,
                sale_date
            )
            
            sales_count += 1
        
        # Verify sales were recorded
        query = "SELECT COUNT(*) FROM event_sales WHERE event_id = ?"
        count = self.db_manager.execute_query(query, (event_id,))
        
        if not count or count[0][0] == 0:
            raise ValueError(f"No sales recorded for event {event_id}")
        
        return count[0][0]
    
    def _test_excel_generation(self, event_id: int) -> str:
        """Test Excel generation functionality"""
        # Generate Excel file
        output_path = os.path.join(self.temp_dir, f"test_event_{event_id}.xlsx")
        
        excel_path = self.excel_generator.generate_event_excel(
            event_id,
            output_path
        )
        
        # Verify file was created
        if not os.path.exists(excel_path):
            raise ValueError(f"Excel file was not generated at {excel_path}")
        
        # Verify file contains data
        try:
            df = pd.read_excel(excel_path)
            if df.empty:
                raise ValueError(f"Generated Excel file is empty")
        except Exception as e:
            raise ValueError(f"Error reading generated Excel file: {str(e)}")
        
        return excel_path
    
    def _test_statistics_generation(self, event_id: int) -> str:
        """Test statistics generation functionality"""
        # Generate statistics report
        output_path = self.event_statistics.generate_event_performance_report(
            event_id,
            output_format='excel'
        )
        
        # Verify file was created
        if not os.path.exists(output_path):
            raise ValueError(f"Statistics report was not generated at {output_path}")
        
        # Verify file contains data
        try:
            df = pd.read_excel(output_path)
            if df.empty:
                raise ValueError(f"Generated statistics report is empty")
        except Exception as e:
            raise ValueError(f"Error reading generated statistics report: {str(e)}")
        
        return output_path
    
    def _test_annual_report(self) -> str:
        """Test annual report generation functionality"""
        # Generate annual report
        current_year = datetime.datetime.now().year
        output_path = self.event_statistics.generate_annual_sales_report(
            current_year,
            output_format='excel'
        )
        
        # Verify file was created
        if not os.path.exists(output_path):
            raise ValueError(f"Annual report was not generated at {output_path}")
        
        # Verify file contains data
        try:
            with pd.ExcelFile(output_path) as xls:
                sheet_names = xls.sheet_names
                if not sheet_names:
                    raise ValueError(f"Generated annual report has no sheets")
                
                # Check at least one sheet has data
                has_data = False
                for sheet in sheet_names:
                    df = pd.read_excel(output_path, sheet_name=sheet)
                    if not df.empty:
                        has_data = True
                        break
                
                if not has_data:
                    raise ValueError(f"All sheets in annual report are empty")
        except Exception as e:
            raise ValueError(f"Error reading generated annual report: {str(e)}")
        
        return output_path
    
    def cleanup(self):
        """Clean up temporary files"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)


class EventValidatorTests(unittest.TestCase):
    """Unit tests for the event workflow validator"""
    
    def setUp(self):
        self.validator = EventWorkflowValidator()
    
    def tearDown(self):
        self.validator.cleanup()
    
    def test_complete_workflow(self):
        """Test the complete event management workflow"""
        results = self.validator.validate_complete_workflow()
        self.assertEqual(results['overall_status'], 'PASSED')
        self.assertEqual(len(results['steps']), 6)
        
        for step in results['steps']:
            self.assertEqual(step['status'], 'PASSED')


if __name__ == "__main__":
    # Run validation
    validator = EventWorkflowValidator()
    results = validator.validate_complete_workflow()
    
    print(f"Event Management Workflow Validation: {results['overall_status']}")
    print("-" * 50)
    
    for step in results['steps']:
        print(f"{step['name']}: {step['status']}")
        print(f"  {step['message']}")
    
    print("-" * 50)
    for message in results['messages']:
        print(message)
    
    validator.cleanup()
    
    # Exit with appropriate code
    sys.exit(0 if results['overall_status'] == 'PASSED' else 1)

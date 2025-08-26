import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import datetime

# Import custom modules
from database_schema import DatabaseManager
from excel_importer import ExcelImporter
from inventory_manager import InventoryManager
from barcode_scanner import BarcodeScanner
from event_manager.annual_event_importer import AnnualEventImporter
from event_manager.event_statistics import EventStatistics

from barcode_scanner import BarcodeScanner

class InventoryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Inventory Management System")
        self.root.geometry("1200x700")
        self.root.minsize(1000, 600)
        
        # Initialize database and managers
        self.db_manager = DatabaseManager()
        self.db_manager.create_tables()
        self.inventory_manager = InventoryManager(self.db_manager)
        self.excel_importer = ExcelImporter(self.db_manager)
        self.annual_event_importer = AnnualEventImporter(self.db_manager.get_connection())
        self.event_statistics = EventStatistics(self.db_manager.get_connection())
        
        # Create main frame
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self.dashboard_tab = ttk.Frame(self.notebook)
        self.inventory_tab = ttk.Frame(self.notebook)
        self.import_tab = ttk.Frame(self.notebook)
        self.events_tab = ttk.Frame(self.notebook)
        self.reports_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.dashboard_tab, text="Dashboard")
        self.notebook.add(self.inventory_tab, text="Inventory")
        self.notebook.add(self.import_tab, text="Import Data")
        self.notebook.add(self.events_tab, text="Events")
        self.notebook.add(self.reports_tab, text="Reports")
        
        # Setup each tab
        self.setup_dashboard_tab()
        self.setup_inventory_tab()
        self.setup_import_tab()
        self.setup_events_tab()
        self.setup_reports_tab()
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Load initial data
        self.refresh_inventory_data()
        self.refresh_dashboard()
        self.refresh_events_data()

    def setup_dashboard_tab(self):
        # ... (existing code for dashboard tab)
        pass

    def setup_inventory_tab(self):
        # ... (existing code for inventory tab)
        pass

    def setup_import_tab(self):
        # ... (existing code for import tab)
        pass

    def setup_events_tab(self):
        # Frame for event import
        import_frame = ttk.LabelFrame(self.events_tab, text="Import Annual Events Data")
        import_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(import_frame, text="Excel File:").pack(side=tk.LEFT, padx=5)
        self.event_file_path_var = tk.StringVar()
        ttk.Entry(import_frame, textvariable=self.event_file_path_var, width=60).pack(side=tk.LEFT, padx=5)
        
        browse_event_button = ttk.Button(import_frame, text="Browse", command=self.browse_event_excel_file)
        browse_event_button.pack(side=tk.LEFT, padx=5)
        
        import_event_button = ttk.Button(import_frame, text="Import Events", command=self.import_annual_events)
        import_event_button.pack(side=tk.LEFT, padx=5)

        # Frame for event listing
        events_list_frame = ttk.LabelFrame(self.events_tab, text="Annual Events List")
        events_list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        columns = (
            "id", "periodo", "accordo", "num_eventi", "data", "expo_periodo", "nome",
            "mezzo_trasporto", "disciplina", "localita", "regione", "expo_brand",
            "addetto", "pernotto", "vitto_alloggio", "treno", "spazio_varie",
            "incassi_2024", "caschi", "occhiali", "pneumatici", "bdg_incassi",
            "bdg_costi", "km", "gasolio", "autostrada", "costi_reali", "incassi",
            "pos", "cash", "extra", "vendita_privati_agenti", "ffwd"
        )
        self.events_tree = ttk.Treeview(events_list_frame, columns=columns, show="headings")

        for col in columns:
            self.events_tree.heading(col, text=col.replace("_", " ").title())
            self.events_tree.column(col, width=100)

        y_scrollbar = ttk.Scrollbar(events_list_frame, orient=tk.VERTICAL, command=self.events_tree.yview)
        self.events_tree.configure(yscroll=y_scrollbar.set)
        x_scrollbar = ttk.Scrollbar(events_list_frame, orient=tk.HORIZONTAL, command=self.events_tree.xview)
        self.events_tree.configure(xscroll=x_scrollbar.set)

        self.events_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        # Frame for statistics
        stats_frame = ttk.LabelFrame(self.events_tab, text="Annual Event Statistics")
        stats_frame.pack(fill=tk.X, padx=10, pady=10)

        ttk.Label(stats_frame, text="Year:").pack(side=tk.LEFT, padx=5)
        self.stats_year_var = tk.StringVar(value=str(datetime.datetime.now().year))
        ttk.Entry(stats_frame, textvariable=self.stats_year_var, width=10).pack(side=tk.LEFT, padx=5)
        
        generate_stats_button = ttk.Button(stats_frame, text="Generate Statistics", command=self.generate_annual_event_statistics)
        generate_stats_button.pack(side=tk.LEFT, padx=5)

        self.stats_output_text = tk.Text(stats_frame, wrap=tk.WORD, height=10)
        self.stats_output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def setup_reports_tab(self):
        # ... (existing code for reports tab)
        pass

    # Existing methods (refresh_dashboard, refresh_inventory_data, search_inventory, etc.)
    # ...

    def browse_event_excel_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Annual Events Excel File",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.event_file_path_var.set(file_path)

    def import_annual_events(self):
        file_path = self.event_file_path_var.get()
        if not file_path:
            messagebox.showerror("Error", "Please select an Excel file to import.")
            return
        
        try:
            self.annual_event_importer.import_events_from_excel(file_path)
            messagebox.showinfo("Success", "Annual events imported successfully!")
            self.refresh_events_data()
        except Exception as e:
            messagebox.showerror("Import Error", f"Failed to import events: {e}")

    def refresh_events_data(self):
        for item in self.events_tree.get_children():
            self.events_tree.delete(item)
        
        events = self.db_manager.cursor.execute("SELECT * FROM annual_events").fetchall()
        for event in events:
            self.events_tree.insert("", tk.END, values=event)

    def generate_annual_event_statistics(self):
        try:
            year = int(self.stats_year_var.get())
            summary = self.event_statistics.get_annual_summary(year)
            
            self.stats_output_text.delete(1.0, tk.END)
            self.stats_output_text.insert(tk.END, f"--- Annual Summary for {year} ---\n")
            for key, value in summary.items():
                self.stats_output_text.insert(tk.END, f"{key.replace('_', ' ').title()}: {value}\n")

        except ValueError:
            messagebox.showerror("Input Error", "Please enter a valid year.")
        except Exception as e:
            messagebox.showerror("Statistics Error", f"Failed to generate statistics: {e}")


if __name__ == "__main__":
    root = tk.Tk()
    app = InventoryApp(root)
    root.mainloop()



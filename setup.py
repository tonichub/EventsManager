#!/usr/bin/env python3
"""
Inventory Manager - Installation and Setup Script
This script installs dependencies and sets up the inventory management system.
"""

import os
import sys
import subprocess
import shutil

def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 6):
        print("Error: Python 3.6 or higher is required.")
        sys.exit(1)
    print(f"Python version {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} detected.")

def install_dependencies():
    """Install required Python packages."""
    print("Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError:
        print("Error: Failed to install dependencies.")
        sys.exit(1)

def create_data_directory():
    """Create data directory if it doesn't exist."""
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created data directory: {data_dir}")
    else:
        print(f"Data directory already exists: {data_dir}")

def setup():
    """Run the setup process."""
    print("Setting up Inventory Manager...")
    check_python_version()
    install_dependencies()
    create_data_directory()
    print("\nSetup completed successfully!")
    print("\nTo start the application, run:")
    print("python src/main.py")

if __name__ == "__main__":
    setup()

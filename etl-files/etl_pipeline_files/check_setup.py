#!/usr/bin/env python3
"""
Diagnostic script to check ETL pipeline setup
"""
import sys
from pathlib import Path

print("="*80)
print("ETL Pipeline Setup Checker")
print("="*80)
print()

# Check Python version
print("1. Python Version:")
print(f"   {sys.version}")
print()

# Check current directory
print("2. Current Directory:")
print(f"   {Path.cwd()}")
print()

# Check required files
print("3. Required Files:")
required_files = [
    'main.py',
    '.env',
    'api-ref.txt',
    'config/database.py',
    'config/api_config.py',
    'extract/crimes_extractor.py',
    'load/files_loader.py',
    'utils/logger.py'
]

for file in required_files:
    path = Path(file)
    exists = path.exists()
    status = "✓" if exists else "✗"
    print(f"   {status} {file}")
print()

# Check Python packages
print("4. Required Python Packages:")
packages = ['psycopg2', 'requests', 'dotenv']
for pkg in packages:
    try:
        __import__(pkg.replace('-', '_'))
        print(f"   ✓ {pkg}")
    except ImportError:
        print(f"   ✗ {pkg} (MISSING - run: pip install {pkg})")
print()

# Check imports
print("5. Testing Imports:")
try:
    sys.path.insert(0, str(Path(__file__).parent))
    
    try:
        from config.database import get_db_config
        print("   ✓ config.database")
    except Exception as e:
        print(f"   ✗ config.database: {e}")
    
    try:
        from config.api_config import APIConfig
        print("   ✓ config.api_config")
    except Exception as e:
        print(f"   ✗ config.api_config: {e}")
    
    try:
        from utils.logger import setup_logger
        print("   ✓ utils.logger")
    except Exception as e:
        print(f"   ✗ utils.logger: {e}")
    
    try:
        from extract.crimes_extractor import CrimesExtractor
        print("   ✓ extract.crimes_extractor")
    except Exception as e:
        print(f"   ✗ extract.crimes_extractor: {e}")
    
    try:
        from load.files_loader import FilesLoader
        print("   ✓ load.files_loader")
    except Exception as e:
        print(f"   ✗ load.files_loader: {e}")

except Exception as e:
    print(f"   ✗ Import error: {e}")
print()

# Check .env file
print("6. .env File Check:")
env_file = Path('.env')
if env_file.exists():
    print("   ✓ .env file exists")
    try:
        from dotenv import load_dotenv
        import os
        load_dotenv()
        required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
        for var in required_vars:
            value = os.getenv(var)
            if value:
                print(f"   ✓ {var} is set")
            else:
                print(f"   ✗ {var} is missing")
    except Exception as e:
        print(f"   ✗ Error loading .env: {e}")
else:
    print("   ✗ .env file not found")
print()

# Check api-ref.txt
print("7. api-ref.txt Check:")
api_ref = Path('api-ref.txt')
if api_ref.exists():
    print("   ✓ api-ref.txt exists")
    try:
        from config.api_config import APIConfig
        config = APIConfig('api-ref.txt')
        print(f"   ✓ Base URL: {config.base_url}")
        print(f"   ✓ Endpoints: {list(config.endpoints.keys())}")
    except Exception as e:
        print(f"   ✗ Error parsing api-ref.txt: {e}")
else:
    print("   ✗ api-ref.txt not found")
print()

print("="*80)
print("Setup check complete!")
print("="*80)



"""
Database configuration from .env file
"""
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_config():
    """
    Get database configuration from environment variables.
    
    Returns:
        dict: Database connection parameters
    """
    config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Validate required fields
    required_fields = ['host', 'database', 'user', 'password']
    missing = [field for field in required_fields if not config.get(field)]
    
    if missing:
        raise ValueError(f"Missing required database configuration: {', '.join(missing)}")
    
    return config


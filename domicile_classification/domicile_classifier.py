#!/usr/bin/env python3
"""
Domicile Classification Script
Classifies persons based on their permanent_state_ut and permanent_country values.
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('domicile_classification.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Indian States and Union Territories
INDIAN_STATES = {
    # States (28)
    "andhra pradesh",
    "arunachal pradesh",
    "assam",
    "bihar",
    "chhattisgarh",
    "goa",
    "gujarat",
    "haryana",
    "himachal pradesh",
    "jharkhand",
    "karnataka",
    "kerala",
    "madhya pradesh",
    "maharashtra",
    "manipur",
    "meghalaya",
    "mizoram",
    "nagaland",
    "odisha",
    "punjab",
    "rajasthan",
    "sikkim",
    "tamil nadu",
    "telangana",
    "tripura",
    "uttar pradesh",
    "uttarakhand",
    "west bengal",
    # Union Territories (8)
    "andaman and nicobar islands",
    "chandigarh",
    "dadra and nagar haveli and daman and diu",
    "delhi",
    "national capital territory",
    "jammu and kashmir",
    "ladakh",
    "lakshadweep",
    "puducherry"
}

# Native state
NATIVE_STATE = "telangana"

# Classification constants
CLASSIFICATION_NATIVE = "native state"
CLASSIFICATION_INTER = "inter state"
CLASSIFICATION_INTERNATIONAL = "international"


def get_db_connection():
    """Create and return a database connection using credentials from .env file."""
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        logger.info("Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def check_and_add_domicile_column(cursor):
    """
    Check if domicile_classification column exists in persons table.
    If not available, add the column. If available, use existing column.
    """
    try:
        logger.info("Checking if domicile_classification column exists in persons table...")
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_name='persons' AND column_name='domicile_classification';
        """)
        
        column_info = cursor.fetchone()
        
        if column_info is None:
            # Column does not exist - add it
            logger.info("Column 'domicile_classification' NOT found in persons table")
            logger.info("Adding domicile_classification column to persons table...")
            
            cursor.execute("""
                ALTER TABLE persons 
                ADD COLUMN domicile_classification VARCHAR(50);
            """)
            
            logger.info("✓ Column 'domicile_classification' added successfully (VARCHAR(50))")
            
            # Verify the column was added
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns 
                WHERE table_name='persons' AND column_name='domicile_classification';
            """)
            verify = cursor.fetchone()
            if verify:
                logger.info(f"✓ Verified: Column exists - {verify['data_type']}({verify['character_maximum_length']})")
            else:
                logger.warning("⚠ Warning: Could not verify column was added")
        else:
            # Column already exists - use it
            logger.info("Column 'domicile_classification' already exists in persons table")
            logger.info(f"  - Data Type: {column_info['data_type']}")
            logger.info(f"  - Max Length: {column_info['character_maximum_length']}")
            logger.info("Using existing column for classification updates")
            
    except Exception as e:
        logger.error(f"Error checking/adding column: {e}")
        raise


def normalize_text(text: Optional[str]) -> Optional[str]:
    """
    Normalize text for comparison:
    - Convert to lowercase
    - Strip whitespace
    - Return None for NULL, empty, or 'default' values
    """
    if text is None or text.strip() == '' or text.strip().lower() == 'default':
        return None
    return text.strip().lower()


def classify_domicile(permanent_state_ut: Optional[str], permanent_country: Optional[str]) -> Optional[str]:
    """
    Classify domicile based on permanent_state_ut and permanent_country.
    
    Logic:
    1. If state/country is NULL, empty, or 'default' -> return NULL
    2. If permanent_country != "India" -> "international"
    3. If permanent_state_ut == "Telangana" -> "native state"
    4. If permanent_state_ut is in Indian states/UTs list -> "inter state"
    5. Otherwise -> "international"
    """
    # Normalize inputs
    state = normalize_text(permanent_state_ut)
    country = normalize_text(permanent_country)
    
    # If both are NULL/empty/default, return NULL
    if state is None and country is None:
        return None
    
    # If country is explicitly not India, it's international
    if country is not None and country != "india":
        return CLASSIFICATION_INTERNATIONAL
    
    # If state is NULL/empty/default, return NULL
    if state is None:
        return None
    
    # Check if it's Telangana (native state)
    if state == NATIVE_STATE:
        return CLASSIFICATION_NATIVE
    
    # Check if it's another Indian state/UT
    if state in INDIAN_STATES:
        return CLASSIFICATION_INTER
    
    # If state is not in Indian states list, it's international
    return CLASSIFICATION_INTERNATIONAL


def process_persons(cursor):
    """Process all persons and classify their domicile."""
    try:
        # Fetch all persons with relevant data
        logger.info("Fetching persons data...")
        cursor.execute("""
            SELECT person_id, permanent_state_ut, permanent_country 
            FROM persons
            ORDER BY person_id;
        """)
        
        persons = cursor.fetchall()
        total_persons = len(persons)
        logger.info(f"Found {total_persons} persons to process")
        
        # Statistics
        stats = {
            CLASSIFICATION_NATIVE: 0,
            CLASSIFICATION_INTER: 0,
            CLASSIFICATION_INTERNATIONAL: 0,
            'null': 0
        }
        
        # Process each person
        for idx, person in enumerate(persons, 1):
            person_id = person['person_id']
            state = person['permanent_state_ut']
            country = person['permanent_country']
            
            # Classify
            classification = classify_domicile(state, country)
            
            # Update statistics
            if classification is None:
                stats['null'] += 1
            else:
                stats[classification] += 1
            
            # Update database
            cursor.execute("""
                UPDATE persons 
                SET domicile_classification = %s 
                WHERE person_id = %s;
            """, (classification, person_id))
            
            # Log progress every 100 records
            if idx % 100 == 0:
                logger.info(f"Processed {idx}/{total_persons} persons...")
        
        logger.info(f"Completed processing all {total_persons} persons")
        logger.info(f"Classification Statistics:")
        logger.info(f"  - Native State: {stats[CLASSIFICATION_NATIVE]}")
        logger.info(f"  - Inter State: {stats[CLASSIFICATION_INTER]}")
        logger.info(f"  - International: {stats[CLASSIFICATION_INTERNATIONAL]}")
        logger.info(f"  - NULL/Empty: {stats['null']}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error processing persons: {e}")
        raise


def main():
    """Main function to orchestrate the domicile classification process."""
    logger.info("=" * 60)
    logger.info("Starting Domicile Classification Process")
    logger.info("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please create a .env file with the required database credentials")
        sys.exit(1)
    
    connection = None
    cursor = None
    
    try:
        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Step 1: Check if column exists, if not add it
        logger.info("Step 1: Checking/Adding domicile_classification column...")
        check_and_add_domicile_column(cursor)
        connection.commit()
        logger.info("")
        
        # Step 2: Process all persons and classify
        logger.info("Step 2: Processing persons and classifying domicile...")
        stats = process_persons(cursor)
        connection.commit()
        logger.info("")
        
        logger.info("=" * 60)
        logger.info("Domicile Classification Completed Successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if connection:
            connection.rollback()
            logger.info("Transaction rolled back")
        sys.exit(1)
        
    finally:
        # Close connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()



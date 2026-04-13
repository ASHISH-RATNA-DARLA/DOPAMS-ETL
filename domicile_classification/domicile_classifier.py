#!/usr/bin/env python3
"""
Domicile Classification Script
Classifies persons based on address information using primary-first hierarchy:
1. Use permanent_country first, if not available then present_country
2. If country is non-India: Classify as 'international'
3. If country is India: Check state (permanent_state_ut first, then present_state_ut)
   - 'telangana': 'native state'
   - Other Indian states/UTs: 'inter state'
   - Unrecognized: None
4. Outputs: 'native state' (Telangana only), 'inter state', 'international', or None
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
from typing import Optional
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool, compute_safe_workers

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
    """
    DEPRECATED: Use PostgreSQLConnectionPool.get_connection_context() instead.
    Retained for backward compatibility with main() schema-check step.
    """
    logger.warning("get_db_connection() is deprecated — prefer pool.get_connection_context()")
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


def classify_domicile(
    perm_state: Optional[str], perm_country: Optional[str],
    pres_state: Optional[str], pres_country: Optional[str]
) -> Optional[str]:
    """
    Classify domicile based on country and state information hierarchy.
    
    Logic:
    1. Determine effective country: permanent_country first, if not available then present_country
    2. If effective country is non-India (non-null and != 'india'): Return 'international'
    3. If effective country is India: Check state (permanent_state_ut first, then present_state_ut)
       - If state is 'telangana' (native state): Return 'native state'
       - If state is any other Indian state/UT: Return 'inter state'
       - Otherwise: Return None
    4. If no effective country available: Return None
    """
    # Normalize inputs
    perm_st = normalize_text(perm_state)
    perm_co = normalize_text(perm_country)
    pres_st = normalize_text(pres_state)
    pres_co = normalize_text(pres_country)
    
    # Step 1: Determine effective country (permanent first, then present)
    effective_country = perm_co if perm_co is not None else pres_co
    
    # If no country information available, cannot classify
    if effective_country is None:
        return None
    
    # Step 2: Check if international (country is explicitly not India)
    if effective_country != "india":
        return CLASSIFICATION_INTERNATIONAL
    
    # Step 3: Country is India - determine effective state (permanent first, then present)
    effective_state = perm_st if perm_st is not None else pres_st
    
    # If no state information available, cannot classify as native/inter state
    if effective_state is None:
        return None
    
    # Step 4: Classify based on state
    if effective_state == NATIVE_STATE:
        return CLASSIFICATION_NATIVE
    if effective_state in INDIAN_STATES:
        return CLASSIFICATION_INTER
    
    # Unrecognized state (but country is India)
    return None


def process_persons(cursor=None):
    """Recompute domicile deterministically and update only rows whose value changed."""
    try:
        max_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
        pool = PostgreSQLConnectionPool(minconn=1, maxconn=max_workers + 5)
        
        # Recompute classifications from current geo data so downstream fixes are rerunnable.
        logger.info("Fetching persons data...")
        with pool.get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        person_id,
                        permanent_state_ut,
                        permanent_country,
                        present_state_ut,
                        present_country,
                        domicile_classification
                    FROM persons
                    WHERE
                        NULLIF(TRIM(COALESCE(permanent_state_ut, '')), '') IS NOT NULL
                        OR NULLIF(TRIM(COALESCE(permanent_country, '')), '') IS NOT NULL
                        OR NULLIF(TRIM(COALESCE(present_state_ut, '')), '') IS NOT NULL
                        OR NULLIF(TRIM(COALESCE(present_country, '')), '') IS NOT NULL
                        OR NULLIF(TRIM(COALESCE(domicile_classification, '')), '') IS NOT NULL
                    ORDER BY person_id;
                """)
                persons = cur.fetchall()

        total_persons = len(persons)
        logger.info(f"Found {total_persons} persons to evaluate")
        
        # Statistics
        stats_lock = threading.Lock()
        stats = {
            CLASSIFICATION_NATIVE: 0,
            CLASSIFICATION_INTER: 0,
            CLASSIFICATION_INTERNATIONAL: 0,
            'null': 0,
            'changed': 0
        }

        def process_batch(batch):
            updates = []
            local_stats = {
                CLASSIFICATION_NATIVE: 0,
                CLASSIFICATION_INTER: 0,
                CLASSIFICATION_INTERNATIONAL: 0,
                'null': 0,
                'changed': 0
            }
            for person in batch:
                person_id = person['person_id']
                perm_state = person['permanent_state_ut']
                perm_country = person['permanent_country']
                pres_state = person['present_state_ut']
                pres_country = person['present_country']
                current_classification = normalize_text(person['domicile_classification'])

                # Classify
                classification = classify_domicile(perm_state, perm_country, pres_state, pres_country)
                
                # Update statistics
                if classification is None:
                    local_stats['null'] += 1
                else:
                    local_stats[classification] += 1

                if current_classification != classification:
                    updates.append((classification, person_id))
                    local_stats['changed'] += 1

            with stats_lock:
                for k in stats:
                    stats[k] += local_stats[k]
                    
            if updates:
                with pool.get_connection_context() as conn:
                    with conn.cursor() as update_cur:
                        from psycopg2.extras import execute_batch
                        execute_batch(update_cur, """
                            UPDATE persons 
                            SET domicile_classification = %s 
                            WHERE person_id = %s;
                        """, updates)
                    conn.commit()

        batch_size = 2500
        batches = [persons[i:i + batch_size] for i in range(0, total_persons, batch_size)]
        
        requested_workers = int(os.environ.get('MAX_WORKERS', min(32, (os.cpu_count() or 1) * 4)))
        safe_workers = compute_safe_workers(pool, requested_workers)
        with ThreadPoolExecutor(max_workers=safe_workers) as executor:
            futures = {executor.submit(process_batch, batch): i for i, batch in enumerate(batches)}
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                    if i % 10 == 0 or i == len(batches):
                        processed = min(i * batch_size, total_persons)
                        logger.info(f"Processed {processed}/{total_persons} persons...")
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
        
        logger.info(f"Completed processing all {total_persons} persons")
        logger.info(f"Classification Statistics:")
        logger.info(f"  - Native State: {stats[CLASSIFICATION_NATIVE]}")
        logger.info(f"  - Inter State: {stats[CLASSIFICATION_INTER]}")
        logger.info(f"  - International: {stats[CLASSIFICATION_INTERNATIONAL]}")
        logger.info(f"  - NULL/Empty: {stats['null']}")
        logger.info(f"  - Rows Updated: {stats['changed']}")

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
        # connection.commit()  # Commits are handled inside process_persons using db pool
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


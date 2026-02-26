
import sys
import logging
from db import get_db_connection, fetch_crimes_by_ids, insert_drug_facts, fetch_unprocessed_crimes
from extractor import extract_drug_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    logging.info("Starting Drug Extraction Service...")
    
    # 1. Connect to DB
    try:
        conn = get_db_connection()
        logging.info("Database connection established.")
    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        sys.exit(1)

    # 2. Processing Loop
    try:
        crimes = []
        input_file = "input.txt"
        crime_ids = []
        
        # Try to read input.txt
        try:
            with open(input_file, "r") as f:
                crime_ids = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        except FileNotFoundError:
            logging.info(f"{input_file} not found. Will fetch unprocessed crimes from DB.")

        if crime_ids:
            # Manual Mode: Process specific IDs from file
            logging.info(f"Read {len(crime_ids)} IDs from {input_file}. Fetching from DB...")
            crimes = fetch_crimes_by_ids(conn, crime_ids)
            process_crimes(conn, crimes)
        else:
            # Dynamic Mode: Process ALL unprocessed crimes in batches
            logging.info("No input IDs provided. Starting Dynamic Batch Processing...")
            batch_size = 100
            total_processed = 0
            
            while True:
                crimes = fetch_unprocessed_crimes(conn, limit=batch_size)
                if not crimes:
                    logging.info("No more unprocessed crimes found in DB. Exiting.")
                    break
                    
                logging.info(f"Fetched batch of {len(crimes)} unprocessed crimes.")
                process_crimes(conn, crimes)
                total_processed += len(crimes)
                logging.info(f"Batch complete. Total processed so far: {total_processed}")
                
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed.")

def process_crimes(conn, crimes):
    for crime in crimes:
        crime_id = crime['crime_id']
        facts_text = crime['brief_facts']
        
        logging.info(f"Processing Crime ID: {crime_id}")
        
        # 3. Extract Info
        try:
            extractions = extract_drug_info(facts_text)
        except Exception as e:
            logging.error(f"Extraction failed for Crime {crime_id}: {e}")
            continue

        # 4. Filter and Insert
        if not extractions:
            logging.info(f"No drugs found for Crime {crime_id}.")
            # Fallthrough to placeholder insertion logic below
            
        # Drug names that must be rejected before any insert
        INVALID_DRUG_NAMES = {
            'unknown', 'unidentified', 'unknown drug', 'unknown substance',
            'unknown tablet', 'unknown powder', 'unknown liquid', 'n/a', 'none', ''
        }

        count = 0
        for drug in extractions:
            # Guard: reject vague/placeholder drug names
            if drug.drug_name.strip().lower() in INVALID_DRUG_NAMES:
                logging.info(f"Skipping invalid drug name '{drug.drug_name}' for Crime {crime_id}.")
                continue

            # User Requirement: Confidence score check (90+)
            if drug.confidence_score >= 90:
                insert_drug_facts(conn, crime_id, drug.model_dump())
                count += 1
            else:
                logging.info(f"Skipping low confidence extraction ({drug.confidence_score}%): {drug.drug_name}")
        
        # CRITICAL: If no drugs were inserted (either none found, or all low confidence),
        # we MUST insert a placeholder to mark this crime as "processed".
        # Otherwise, fetch_unprocessed_crimes will pick it up again forever (Infinite Loop).
        if count == 0:
            logging.info(f"Marking Crime {crime_id} as processed (NO_DRUGS_DETECTED).")
            placeholder = {
                "drug_name": "NO_DRUGS_DETECTED",
                "quantity_numeric": 0,
                "quantity_unit": "None",
                "standardized_quantity_kg": 0,
                "standardized_unit": "Count",
                "drug_form": "None",
                "packaging_details": "None",
                "confidence_score": 100,
                "extraction_metadata": {}
            }
            insert_drug_facts(conn, crime_id, placeholder)

        logging.info(f"Completed processing for Crime {crime_id}. inserted_count={count}")

if __name__ == "__main__":
    main()


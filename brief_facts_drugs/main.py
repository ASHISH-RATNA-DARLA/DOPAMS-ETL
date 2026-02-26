
import sys
import logging
from db import get_db_connection, fetch_crimes_by_ids, insert_drug_facts, fetch_unprocessed_crimes, fetch_drug_categories
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
        
        # 1.5 Fetch Knowledge Base
        drug_categories = fetch_drug_categories(conn)
        logging.info(f"Loaded {len(drug_categories)} drug categories from knowledge base.")
        
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
            process_crimes(conn, crimes, drug_categories)
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
                process_crimes(conn, crimes, drug_categories)
                total_processed += len(crimes)
                logging.info(f"Batch complete. Total processed so far: {total_processed}")
                
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed.")

def process_crimes(conn, crimes, drug_categories=None):
    if drug_categories is None:
        drug_categories = []
        
    for crime in crimes:
        crime_id = crime['crime_id']
        facts_text = crime['brief_facts']
        
        logging.info(f"Processing Crime ID: {crime_id}")
        
        # 3. Extract Info
        try:
            extractions = extract_drug_info(facts_text, drug_categories)
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
            if drug.primary_drug_name.strip().lower() in INVALID_DRUG_NAMES:
                logging.info(f"Skipping invalid drug name '{drug.primary_drug_name}' for Crime {crime_id}.")
                continue

            # User Requirement: Confidence score check (90+)
            if drug.confidence_score >= 90:
                insert_drug_facts(conn, crime_id, drug.model_dump())
                count += 1
            else:
                logging.info(f"Skipping low confidence extraction ({drug.confidence_score}%): {drug.primary_drug_name}")
        
        # CRITICAL: If no drugs were inserted (either none found, or all low confidence),
        # we MUST insert a placeholder to mark this crime as "processed".
        # Otherwise, fetch_unprocessed_crimes will pick it up again forever (Infinite Loop).
        if count == 0:
            logging.info(f"Marking Crime {crime_id} as processed (NO_DRUGS_DETECTED).")
            placeholder = {
                "raw_drug_name": "NO_DRUGS_DETECTED",
                "raw_quantity": 0,
                "raw_unit": "None",
                "primary_drug_name": "NO_DRUGS_DETECTED",
                "drug_form": "None",
                "accused_id": None,
                "weight_g": 0,
                "weight_kg": 0,
                "volume_ml": 0,
                "volume_l": 0,
                "count_total": 0,
                "confidence_score": 100,
                "extraction_metadata": {"source_sentence": "Placeholder for zero detections"},
                "is_commercial": False,
                "seizure_worth": 0.0
            }
            insert_drug_facts(conn, crime_id, placeholder)

        logging.info(f"Completed processing for Crime {crime_id}. inserted_count={count}")

if __name__ == "__main__":
    main()


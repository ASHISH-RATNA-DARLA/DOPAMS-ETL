
import sys
import logging
from fuzzywuzzy import fuzz
from db import get_db_connection, fetch_crimes_by_ids, insert_accused_facts, fetch_unprocessed_crimes, fetch_existing_accused_for_crime
from extractor import extract_accused_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def find_best_match(extracted_name, existing_records, threshold=85):
    """
    Finds the best matching person from existing records using fuzzy matching on name/alias.
    """
    if not extracted_name or not existing_records:
        return None
        
    best_match = None
    highest_score = 0
    
    extracted_clean = extracted_name.lower().strip()
    
    for record in existing_records:
        # Check Full Name
        db_name = (record.get('full_name') or "").lower()
        score_name = fuzz.token_sort_ratio(extracted_clean, db_name)
        
        # Check Alias
        db_alias = (record.get('alias') or "").lower()
        score_alias = 0
        if db_alias:
            score_alias = fuzz.token_sort_ratio(extracted_clean, db_alias)
            
        current_max = max(score_name, score_alias)
        
        if current_max > highest_score:
            highest_score = current_max
            best_match = record
            
    if highest_score >= threshold:
        return best_match
    return None

def main():
    logging.info("Starting Accused Extraction Service...")
    
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
            batch_size = 50 
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
        facts_text = crime['brief_facts'] or ""
        
        logging.info(f"Processing Crime ID: {crime_id}")
        
        # 3. Extract Info
        try:
            extractions = extract_accused_info(facts_text)
        except Exception as e:
            logging.error(f"Extraction failed for Crime {crime_id}: {e}")
            continue

        # 4. Fetch Existing Accused for Matching
        existing_records = fetch_existing_accused_for_crime(conn, crime_id)
        
        # 5. Process & Insert
        count = 0
        if not extractions:
             logging.info(f"No accused found for Crime {crime_id} (Extraction returned empty).")
             # We should probably insert a placeholder so it doesn't get picked up again
             # Creating a placeholder "Processed" entry
             placeholder = {
                "crime_id": crime_id,
                "full_name": "NO_ACCUSED_FOUND",
                "existing_accused": False
             }
             insert_accused_facts(conn, placeholder)
        
        for accused in extractions:
            data = accused.model_dump()
            data['crime_id'] = crime_id
            
            # --- Matching Logic ---
            match = find_best_match(accused.full_name, existing_records)
            
            if match:
                data['person_id'] = match['person_id']
                data['accused_id'] = match['accused_id']
                data['existing_accused'] = True
                
                # --- ENRICHMENT LOGIC ---
                # Fill missing details from existing person record
                # Fields to enrich: age, gender, occupation, address, phone_numbers
                # Rule: Only fill if extracted is None/Null.
                
                enrichable_fields = ['age', 'gender', 'occupation', 'address', 'phone_numbers']
                for field in enrichable_fields:
                    if not data.get(field) and match.get(field):
                        data[field] = match[field]
                        
                # CRITICAL: Do NOT overwrite 'accused_type'. 
                # User Rule: "accused type must be llm provided one only"
                # So we keep data['accused_type'] as extracted.
                
            else:
                data['existing_accused'] = False
            
            # SANITIZATION: Handle 'unknown' type for DB Constraint
            # DB Check: accused_type IN ('peddler', ...) OR NULL
            if data.get('accused_type') == 'unknown':
                data['accused_type'] = None
                
            insert_accused_facts(conn, data)
            count += 1
            
        logging.info(f"Completed processing for Crime {crime_id}. inserted_count={count}")

if __name__ == "__main__":
    main()


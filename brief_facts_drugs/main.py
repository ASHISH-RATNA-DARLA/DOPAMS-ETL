
import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import (get_db_connection, fetch_crimes_by_ids, insert_drug_facts,
                fetch_unprocessed_crimes, fetch_drug_categories, ensure_connection,
                batch_insert_drug_facts, fetch_ignored_checklist, is_drug_ignored)
from extractor import extract_drug_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------------------------------------------------------
# Parallelism configuration
# ---------------------------------------------------------------------------
# PARALLEL_LLM_WORKERS: number of concurrent LLM requests.
# Must match Ollama server's OLLAMA_NUM_PARALLEL env var.
# Guideline per GPU VRAM (qwen2.5-coder:14b, num_ctx=16384):
#   24 GB VRAM → 3 workers    (model ~9GB + 3×~2GB KV cache)
#   48 GB VRAM → 6 workers
#   80 GB VRAM → 10 workers
import os
# Optimized for 64GB RAM server with decent VRAM
PARALLEL_LLM_WORKERS = int(os.getenv("PARALLEL_LLM_WORKERS", "3"))


def main():
    logging.info("Starting Drug Extraction Service...")
    logging.info(f"Parallel LLM workers: {PARALLEL_LLM_WORKERS}")

    # 1. Connect to DB
    try:
        conn = get_db_connection()
        logging.info("Database connection established.")

        # 1.5 Fetch Knowledge Base (one-time)
        drug_categories = fetch_drug_categories(conn)
        logging.info(f"Loaded {len(drug_categories)} drug categories from knowledge base.")

        # 1.6 Fetch Ignored Checklist (one-time)
        ignored_checklist = fetch_ignored_checklist(conn)
        logging.info(f"Loaded {len(ignored_checklist)} ignored drug terms for validation.")

    except Exception as e:
        logging.error(f"Failed to connect to DB: {e}")
        sys.exit(1)

    # 2. Processing Loop
    try:
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
            process_crimes_parallel(conn, crimes, drug_categories, ignored_checklist)
        else:
            # Dynamic Mode: Process ALL unprocessed crimes in batches
            logging.info("No input IDs provided. Starting Dynamic Batch Processing...")
            # Increased batch size for 64GB server throughput
            batch_size = int(os.getenv("BATCH_SIZE", "15"))
            total_processed = 0

            while True:
                try:
                    conn = ensure_connection(conn)
                    crimes = fetch_unprocessed_crimes(conn, limit=batch_size)
                    if not crimes:
                        logging.info("No more unprocessed crimes found in DB. Exiting.")
                        break

                    logging.info(f"Fetched batch of {len(crimes)} unprocessed crimes.")
                    process_crimes_parallel(conn, crimes, drug_categories, ignored_checklist)
                    total_processed += len(crimes)
                    logging.info(f"Batch complete. Total processed so far: {total_processed}")
                except Exception as e:
                    logging.error(f"Batch processing error: {e}. Attempting reconnection...")
                    try:
                        conn = get_db_connection()
                        logging.info("Reconnected to database. Resuming...")
                    except Exception as reconnect_err:
                        logging.error(f"Reconnection failed: {reconnect_err}. Exiting.")
                        break

    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
    finally:
        conn.close()
        logging.info("Database connection closed.")


# ---------------------------------------------------------------------------
# Placeholder record for crimes with no drug content
# ---------------------------------------------------------------------------
_NO_DRUGS_PLACEHOLDER = {
    "raw_drug_name": "NO_DRUGS_DETECTED", "raw_quantity": 0, "raw_unit": "None",
    "primary_drug_name": "NO_DRUGS_DETECTED", "drug_form": "None", "accused_id": None,
    "weight_g": 0, "weight_kg": 0, "volume_ml": 0, "volume_l": 0, "count_total": 0,
    "confidence_score": 1.00, "extraction_metadata": {"source_sentence": "Placeholder for zero detections"},
    "is_commercial": False, "seizure_worth": 0.0
}

# Drug names that must be rejected before any insert
INVALID_DRUG_NAMES = {
    'unknown', 'unidentified', 'unknown drug', 'unknown substance',
    'unknown tablet', 'unknown powder', 'unknown liquid', 'n/a', 'none', ''
}


def _extract_single_crime(crime, drug_categories):
    """
    Worker function: preprocess + LLM extract for ONE crime.
    Runs in a thread — no DB writes here (those happen in the main thread).
    Returns (crime_id, list_of_valid_drug_dicts) or (crime_id, None) on error.
    """
    crime_id = crime['crime_id']
    facts_text = crime['brief_facts']

    # Guard: empty brief_facts
    if not facts_text or not facts_text.strip():
        logging.info(f"[Worker] Crime {crime_id}: empty brief_facts → placeholder.")
        return (crime_id, [])

    try:
        t0 = time.time()
        extractions = extract_drug_info(facts_text, drug_categories)
        elapsed = time.time() - t0
        logging.info(f"[Worker] Crime {crime_id}: LLM extraction took {elapsed:.1f}s, got {len(extractions)} raw entries.")

        # Filter invalid / low-confidence
        valid = []
        for drug in extractions:
            if drug.primary_drug_name.strip().lower() in INVALID_DRUG_NAMES:
                continue
            if drug.confidence_score >= 0.50:
                valid.append(drug.model_dump())
            else:
                logging.info(f"[Worker] Crime {crime_id}: skipping low confidence ({drug.confidence_score:.0%}) {drug.primary_drug_name}")

        return (crime_id, valid)

    except Exception as e:
        logging.error(f"[Worker] Crime {crime_id}: extraction error: {e}")
        return (crime_id, None)


def process_crimes_parallel(conn, crimes, drug_categories=None, ignored_checklist=None):
    """
    Process a batch of crimes with parallel LLM extraction and batched DB writes.

    Architecture:
      ┌──────────┐        ┌──────────┐
      │  Thread 1 │─LLM──►│          │
      │  Thread 2 │─LLM──►│  Results  │──► Batch DB Insert (main thread)
      │  Thread N │─LLM──►│  Queue    │
      └──────────┘        └──────────┘

    - LLM calls happen in N parallel threads (I/O-bound, waiting for Ollama HTTP)
    - DB writes are batched in the main thread (single connection, no lock contention)
    - Strict validation: Only inserts drugs with confidence >= 50% OR NOT in ignored_checklist
    - NO_DRUGS_DETECTED placeholders are NEVER inserted (crimes with no drugs are simply skipped)
    """
    if drug_categories is None:
        drug_categories = []
    if ignored_checklist is None:
        ignored_checklist = []

    batch_start = time.time()
    total_crimes = len(crimes)
    total_inserted = 0
    total_skipped = 0
    total_ignored = 0
    pending_inserts = []  # collect (crime_id, drug_data_dict) tuples

    # --- Phase 1: Parallel LLM extraction ---
    with ThreadPoolExecutor(max_workers=PARALLEL_LLM_WORKERS) as executor:
        futures = {
            executor.submit(_extract_single_crime, crime, drug_categories): crime['crime_id']
            for crime in crimes
        }

        for future in as_completed(futures):
            crime_id = futures[future]
            try:
                cid, valid_drugs = future.result()

                if valid_drugs is None:
                    # Extraction error — SKIP (don't insert placeholder)
                    logging.warning(f"Crime {cid}: extraction returned None → skipping (no placeholder)")
                    total_skipped += 1
                elif len(valid_drugs) == 0:
                    # No drugs found — SKIP (don't insert placeholder)
                    logging.info(f"Crime {cid}: no drugs extracted → skipping (no placeholder, strict policy)")
                    total_skipped += 1
                else:
                    # STRICT VALIDATION: Check each drug against ignored checklist
                    valid_for_insert = []
                    for drug_data in valid_drugs:
                        raw_name = drug_data.get('raw_drug_name', '')
                        confidence = float(drug_data.get('confidence_score', 0))
                        
                        # Check if drug is in ignored checklist with >80% match
                        is_ignored, matched_term, similarity = is_drug_ignored(raw_name, ignored_checklist, threshold=0.80)
                        
                        if is_ignored:
                            logging.info(
                                f"Crime {cid}: '{raw_name}' matched ignored list '{matched_term}' "
                                f"({similarity:.0%}) → REJECTING (strict policy)"
                            )
                            total_ignored += 1
                        elif confidence < 0.50:
                            # Low confidence AND not in KB mapping → SKIP
                            logging.info(
                                f"Crime {cid}: '{raw_name}' skipped (low confidence {confidence:.0%}, not in KB)"
                            )
                            total_skipped += 1
                        else:
                            # Valid extraction: confidence >= 50% AND not in ignored list
                            valid_for_insert.append(drug_data)
                    
                    if valid_for_insert:
                        for drug_data in valid_for_insert:
                            pending_inserts.append((cid, drug_data))
                        total_inserted += len(valid_for_insert)
                        logging.info(
                            f"Crime {cid}: {len(valid_for_insert)}/{len(valid_drugs)} drug entries "
                            f"queued for insert (after strict validation)."
                        )
                    else:
                        logging.info(
                            f"Crime {cid}: all {len(valid_drugs)} drugs rejected by strict validation → skipping"
                        )
                        total_skipped += 1

            except Exception as e:
                logging.error(f"Crime {crime_id}: future error: {e}")
                # SKIP on error (don't insert placeholder)
                logging.warning(f"Crime {crime_id}: error handling → skipping (no placeholder)")
                total_skipped += 1

    # --- Phase 2: Batched DB writes (single thread, single connection) ---
    if pending_inserts:
        logging.info(f"Writing {len(pending_inserts)} rows to DB in batch...")
        try:
            conn = ensure_connection(conn)
            batch_insert_drug_facts(conn, pending_inserts)
            logging.info(f"Batch DB write complete: {len(pending_inserts)} rows committed.")
        except Exception as e:
            logging.error(f"Batch DB write failed: {e}. Falling back to per-row inserts...")
            # Fallback: insert one by one
            conn = ensure_connection(conn)
            for crime_id, drug_data in pending_inserts:
                try:
                    insert_drug_facts(conn, crime_id, drug_data)
                except Exception as row_err:
                    logging.error(f"Row insert failed for {crime_id}: {row_err}")

    elapsed = time.time() - batch_start
    rate = total_crimes / elapsed if elapsed > 0 else 0
    logging.info(
        f"Batch done: {total_crimes} crimes in {elapsed:.1f}s "
        f"({rate:.1f} crimes/s) — "
        f"{total_inserted} valid drugs inserted, {total_ignored} ignored (matched >80%), "
        f"{total_skipped} skipped (no drugs/low confidence/errors). STRICT POLICY: NO placeholders."
    )


if __name__ == "__main__":
    main()


import sys
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import (get_db_connection, fetch_crimes_by_ids, insert_drug_facts,
                fetch_unprocessed_crimes, fetch_drug_categories, ensure_connection,
                batch_insert_drug_facts, fetch_drug_ignore_list)
from extractor import extract_drug_info, build_drug_keywords

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
PARALLEL_LLM_WORKERS = int(os.getenv("PARALLEL_LLM_WORKERS", "6"))

if PARALLEL_LLM_WORKERS > 1:
    logging.warning(
        f"PARALLEL_LLM_WORKERS={PARALLEL_LLM_WORKERS} — "
        f"verify Ollama is running with OLLAMA_NUM_PARALLEL>={PARALLEL_LLM_WORKERS}. "
        f"If Ollama uses default (1), all threads queue server-side and "
        f"parallelism has ZERO effect. "
        f"Start Ollama with: OLLAMA_NUM_PARALLEL={PARALLEL_LLM_WORKERS} ollama serve"
    )


def main():
    logging.info("Starting Drug Extraction Service...")
    logging.info(f"Parallel LLM workers: {PARALLEL_LLM_WORKERS}")

    # 1. Connect to DB
    try:
        conn = get_db_connection()
        logging.info("Database connection established.")

        # 1.5 Fetch all reference tables once at startup
        drug_categories = fetch_drug_categories(conn)
        logging.info(f"Loaded {len(drug_categories)} drug categories from knowledge base.")

        # Ignore list: {lowercased_term: reason}
        # Used for exact-match filtering on primary_drug_name AFTER KB lookup.
        # NOTE: Never apply as substring — see analysis notes in db.py.
        ignore_dict = fetch_drug_ignore_list(conn)
        ignore_set = set(ignore_dict.keys())  # O(1) lookup set passed to workers
        logging.info(f"Loaded {len(ignore_set)} terms from drug_ignore_list.")

        # KB exact-match lookup dict: {raw_name_lower: standard_name}
        # Built once here, passed read-only to all worker threads (thread-safe).
        # Used by resolve_primary_drug_name() in extractor.py for deterministic
        # name standardization independent of LLM accuracy.
        kb_lookup = {
            row['raw_name'].lower().strip(): row['standard_name']
            for row in drug_categories
        }
        logging.info(f"Built KB lookup dict with {len(kb_lookup)} raw-name entries.")

        # Dynamic keyword set for the preprocessor FIR section scorer.
        # Replaces static _DRUG_KEYWORDS_TIER1 — now derived from the actual KB
        # so every drug in drug_categories is automatically detectable.
        dynamic_drug_keywords = build_drug_keywords(drug_categories)
        logging.info(f"Built dynamic drug keyword set with {len(dynamic_drug_keywords)} tokens.")

    except Exception as e:
        logging.error(f"Failed to connect to DB or load reference data: {e}")
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
            process_crimes_parallel(conn, crimes, drug_categories,
                                     ignore_set=ignore_set,
                                     kb_lookup=kb_lookup,
                                     dynamic_drug_keywords=dynamic_drug_keywords)
        else:
            # Dynamic Mode: Process ALL unprocessed crimes in batches
            logging.info("No input IDs provided. Starting Dynamic Batch Processing...")
            batch_size = int(os.getenv("BATCH_SIZE", "50"))
            total_processed = 0

            # Prefetch executor — single worker, overlaps next DB fetch with current batch write
            from concurrent.futures import ThreadPoolExecutor as _PrefetchPool
            next_crimes = None

            def _prefetch_unprocessed_crimes(limit: int):
                """
                Prefetch in a separate thread using a separate DB connection.
                psycopg2 connections are not thread-safe; never share `conn` across threads.
                """
                prefetch_conn = None
                try:
                    prefetch_conn = get_db_connection()
                    prefetch_conn = ensure_connection(prefetch_conn)
                    return fetch_unprocessed_crimes(prefetch_conn, limit=limit)
                finally:
                    try:
                        if prefetch_conn is not None:
                            prefetch_conn.close()
                    except Exception:
                        pass

            with _PrefetchPool(max_workers=1) as prefetch_pool:
                while True:
                    try:
                        conn = ensure_connection(conn)

                        # Use prefetched batch if available, else fetch now (first iteration)
                        if next_crimes is not None:
                            crimes = next_crimes
                            next_crimes = None
                        else:
                            crimes = fetch_unprocessed_crimes(conn, limit=batch_size)

                        if not crimes:
                            logging.info("No more unprocessed crimes found in DB. Exiting.")
                            break

                        logging.info(f"Processing batch of {len(crimes)} crimes.")

                        # Fire prefetch for NEXT batch immediately — runs during LLM + write
                        next_fetch = prefetch_pool.submit(
                            _prefetch_unprocessed_crimes,
                            batch_size
                        )

                        # Process current batch (LLM parallel + batch DB write)
                        process_crimes_parallel(conn, crimes, drug_categories,
                                                 ignore_set=ignore_set,
                                                 kb_lookup=kb_lookup,
                                                 dynamic_drug_keywords=dynamic_drug_keywords)
                        total_processed += len(crimes)
                        logging.info(f"Batch complete. Total processed so far: {total_processed}")

                        # Collect prefetched next batch (should already be ready)
                        try:
                            next_crimes = next_fetch.result(timeout=30)
                        except Exception as prefetch_err:
                            logging.warning(f"Prefetch failed: {prefetch_err}. Will fetch inline.")
                            next_crimes = None

                    except Exception as e:
                        logging.error(f"Batch processing error: {e}. Attempting reconnection...")
                        next_crimes = None  # discard prefetch on error
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
    "primary_drug_name": "NO_DRUGS_DETECTED", "drug_form": "None",
    "weight_g": 0, "weight_kg": 0, "volume_ml": 0, "volume_l": 0, "count_total": 0,
    "confidence_score": 1.00, "extraction_metadata": {"source_sentence": "Placeholder for zero detections"},
    "is_commercial": False, "seizure_worth": 0.0
}

# ---------------------------------------------------------------------------
# INVALID_DRUG_NAMES is intentionally REMOVED.
#
# Previously this was a 10-term hardcoded set. Analysis showed 9 of those
# 10 terms are already in drug_ignore_list in the DB. The DB table is now
# the single source of truth — loaded at startup as ignore_set and passed
# into _extract_single_crime(). Adding a new term to the DB table is all
# that's needed; no code changes required.
#
# The only term that was in INVALID_DRUG_NAMES but not the DB was ''.
# That edge case is handled by the `if not primary` guard below.
# ---------------------------------------------------------------------------


def _extract_single_crime(crime, drug_categories, ignore_set=None,
                           kb_lookup=None, dynamic_drug_keywords=None,
                           db_conn=None):
    """
    Worker function: preprocess + LLM extract for ONE crime.
    Runs in a thread — no DB writes here (those happen in the main thread).

    Args:
        crime:                  dict with crime_id and brief_facts
        drug_categories:        list of KB dicts (raw_name, standard_name)
        ignore_set:             set of lowercased terms from drug_ignore_list
                                — exact-matched against primary_drug_name only
        kb_lookup:              dict {raw_name_lower: standard_name} for
                                deterministic name resolution (Tier 1+2)
        dynamic_drug_keywords:  set of tokens for preprocessor FIR scoring
        db_conn:                per-thread DB connection for pg_trgm Tier 3
                                fuzzy name matching in resolve_primary_drug_name

    Returns:
        (crime_id, list_of_valid_drug_dicts)  — valid list (may be empty)
        (crime_id, None)                       — on extraction error
    """
    if ignore_set is None:
        ignore_set = set()
    if kb_lookup is None:
        kb_lookup = {}

    crime_id = crime['crime_id']
    facts_text = crime['brief_facts']

    # Guard: empty brief_facts
    if not facts_text or not facts_text.strip():
        logging.info(f"[Worker] Crime {crime_id}: empty brief_facts → placeholder.")
        return (crime_id, [])

    try:
        t0 = time.time()
        extractions = extract_drug_info(
            facts_text,
            drug_categories,
            ignore_set=ignore_set,
            kb_lookup=kb_lookup,
            dynamic_drug_keywords=dynamic_drug_keywords,
            conn=db_conn,
        )
        elapsed = time.time() - t0
        logging.info(
            f"[Worker] Crime {crime_id}: LLM extraction took {elapsed:.1f}s, "
            f"got {len(extractions)} entries after all filters."
        )

        # Final gate: confidence threshold + empty primary name guard
        valid = []
        for drug in extractions:
            primary = (drug.primary_drug_name or '').strip()
            if not primary:
                logging.info(f"[Worker] Crime {crime_id}: skipping entry with empty primary_drug_name")
                continue
            if drug.confidence_score >= 0.50:
                valid.append(drug.model_dump())
            else:
                logging.info(
                    f"[Worker] Crime {crime_id}: skipping low confidence "
                    f"({drug.confidence_score:.0%}) '{drug.primary_drug_name}'"
                )

        return (crime_id, valid)

    except Exception as e:
        logging.error(f"[Worker] Crime {crime_id}: extraction error: {e}")
        return (crime_id, None)


def process_crimes_parallel(conn, crimes, drug_categories=None,
                              ignore_set=None, kb_lookup=None,
                              dynamic_drug_keywords=None):
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
    - ignore_set, kb_lookup, dynamic_drug_keywords are read-only → thread-safe
    - Worker threads DO NOT hold DB connections (Tier 3 fuzzy matching disabled)
      This eliminates connection pool leakage and keeps pools stable under load.
    """
    if drug_categories is None:
        drug_categories = []
    if ignore_set is None:
        ignore_set = set()
    if kb_lookup is None:
        kb_lookup = {}

    batch_start = time.time()
    total_crimes = len(crimes)
    total_inserted = 0
    total_skipped = 0
    pending_inserts = []  # collect (crime_id, drug_data_dict) tuples

    # --- Phase 1: Parallel LLM extraction ---
    with ThreadPoolExecutor(max_workers=PARALLEL_LLM_WORKERS) as executor:
        futures = {
            executor.submit(
                _extract_single_crime,
                crime,
                drug_categories,
                ignore_set,
                kb_lookup,
                dynamic_drug_keywords,
                db_conn=None,               # Workers do not hold DB connections
            ): crime['crime_id']
            for crime in crimes
        }

        for future in as_completed(futures):
            crime_id = futures[future]
            try:
                cid, valid_drugs = future.result()

                if valid_drugs is None:
                    # Extraction error — insert placeholder
                    pending_inserts.append((cid, _NO_DRUGS_PLACEHOLDER.copy()))
                    total_skipped += 1
                elif len(valid_drugs) == 0:
                    # No drugs found — insert placeholder
                    pending_inserts.append((cid, _NO_DRUGS_PLACEHOLDER.copy()))
                    total_skipped += 1
                else:
                    for drug_data in valid_drugs:
                        pending_inserts.append((cid, drug_data))
                    total_inserted += len(valid_drugs)
                    logging.info(f"Crime {cid}: {len(valid_drugs)} drug entries queued for insert.")

            except Exception as e:
                logging.error(f"Crime {crime_id}: future error: {e}")
                pending_inserts.append((crime_id, _NO_DRUGS_PLACEHOLDER.copy()))
                total_skipped += 1

    # --- Phase 2: Batched DB writes (single thread, single connection) ---
    if pending_inserts:
        # De-duplicate exact row repeats (e.g. reruns or repeated futures)
        # Keep multiplicity only when fields differ (multiple drugs / multiple accused rules).
        deduped = []
        seen = set()
        for cid, drug_data in pending_inserts:
            meta = drug_data.get("extraction_metadata", {}) if isinstance(drug_data, dict) else {}
            src = meta.get("source_sentence") if isinstance(meta, dict) else None
            key = (
                cid,
                str(drug_data.get("raw_drug_name")),
                str(drug_data.get("raw_quantity")),
                str(drug_data.get("raw_unit")),
                str(drug_data.get("primary_drug_name")),
                str(drug_data.get("drug_form")),
                str(drug_data.get("weight_kg")),
                str(drug_data.get("volume_l")),
                str(drug_data.get("count_total")),
                str(drug_data.get("seizure_worth")),
                str(src),
            )
            if key in seen:
                logging.warning(f"Dedup: dropping exact duplicate insert row for crime_id={cid}")
                continue
            seen.add(key)
            deduped.append((cid, drug_data))
        pending_inserts = deduped

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
        f"{total_inserted} drug entries, {total_skipped} no-drug placeholders."
    )


if __name__ == "__main__":
    main()
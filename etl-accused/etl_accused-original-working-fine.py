#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Accused API
Fetches accused data in 5-day chunks and loads into PostgreSQL
Also ensures stub records exist in persons for referenced PERSON_IDs
"""

import sys
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import colorlog
from typing import List, Dict, Optional, Tuple
import json
import os

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# Add TRACE level support (lower than DEBUG)
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, 'TRACE')

def trace(self, message, *args, **kws):
    """Custom trace method for logger"""
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kws)

logging.Logger.trace = trace

# Setup colored logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    LOG_CONFIG['format'],
    datefmt=LOG_CONFIG['date_format'],
    log_colors={
        'TRACE': 'white',
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)

# Handle TRACE level
log_level = LOG_CONFIG['level'].upper()
if log_level == 'TRACE':
    logger.setLevel(TRACE_LEVEL)
else:
    logger.setLevel(log_level)

# Target tables
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')
ACCUSED_TABLE = TABLE_CONFIG.get('accused', 'accused')
PERSONS_TABLE = TABLE_CONFIG.get('persons', 'persons')


def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO 8601 date string (with optional time component) to datetime."""
    if 'T' in date_str:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return datetime.strptime(date_str, '%Y-%m-%d')


class AccusedETL:
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_accused_fetched': 0,
            'total_accused_inserted': 0,
            'total_accused_updated': 0,
            'total_accused_no_change': 0,  # Records that exist but no changes needed
            'total_accused_failed': 0,  # Records that failed to insert/update
            'total_duplicates': 0,  # Duplicate accused_ids found within chunks
            'stub_persons_created': 0,
            'failed_api_calls': 0,
            'accused_without_crime': 0,  # Accused failed due to crime not found
            'accused_without_person': 0,  # Accused failed due to person not found
            'crimes_inserted_from_accused': 0,  # Crimes inserted via fallback when processing accused
            'crimes_updated_from_accused': 0,  # Crimes updated via fallback when processing accused
            'errors': []
        }
        
        # Setup chunk-wise logging files
        self.setup_chunk_loggers()
    
    def setup_chunk_loggers(self):
        """Setup separate log files for API responses, DB operations, and failed records"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # API response log file
        self.api_log_file = f'logs/accused_api_chunks_{timestamp}.log'
        self.api_log = open(self.api_log_file, 'w', encoding='utf-8')
        self.api_log.write(f"# Accused API Chunk-wise Log\n")
        self.api_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.api_log.write(f"# Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
        self.api_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) between chunks)\n")
        self.api_log.write(f"# Expected Total from API Team: 22423 (insert + update records)\n")
        self.api_log.write(f"{'='*80}\n\n")
        
        # Database operations log file
        self.db_log_file = f'logs/accused_db_chunks_{timestamp}.log'
        self.db_log = open(self.db_log_file, 'w', encoding='utf-8')
        self.db_log.write(f"# Accused Database Operations Chunk-wise Log\n")
        self.db_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.db_log.write(f"# Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}\n")
        overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
        self.db_log.write(f"# Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) between chunks)\n")
        self.db_log.write(f"{'='*80}\n\n")
        
        # Failed records log file (records that couldn't be inserted/updated)
        self.failed_log_file = f'logs/accused_failed_{timestamp}.log'
        self.failed_log = open(self.failed_log_file, 'w', encoding='utf-8')
        self.failed_log.write(f"# Accused Failed Records Log\n")
        self.failed_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"# Records that failed to insert or update with reasons\n")
        self.failed_log.write(f"{'='*80}\n\n")
        
        # API response details log file (accused_id, crime_id, person_id)
        self.api_response_file = f'logs/accused_api_response_{timestamp}.log'
        self.api_response_log = open(self.api_response_file, 'w', encoding='utf-8')
        self.api_response_log.write(f"# Accused API Response Details\n")
        self.api_response_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.api_response_log.write(f"# Format: accused_id|crime_id|person_id\n")
        self.api_response_log.write(f"{'='*80}\n\n")
        
        # Fallback failure log file (records that failed even after trying crime_id API)
        self.fallback_failure_file = f'logs/accused_fallback_failures_{timestamp}.log'
        self.fallback_failure_log = open(self.fallback_failure_file, 'w', encoding='utf-8')
        self.fallback_failure_log.write(f"# Accused Fallback Failures Log\n")
        self.fallback_failure_log.write(f"# Generated: {datetime.now().isoformat()}\n")
        self.fallback_failure_log.write(f"# Records that failed even after trying crime_id API endpoint\n")
        self.fallback_failure_log.write(f"# Format: accused_id|person_id|crime_id|reason\n")
        self.fallback_failure_log.write(f"{'='*80}\n\n")
        
        logger.info(f"📝 API chunk log: {self.api_log_file}")
        logger.info(f"📝 DB chunk log: {self.db_log_file}")
        logger.info(f"📝 Failed records log: {self.failed_log_file}")
        logger.info(f"📝 Fallback failures log: {self.fallback_failure_file}")
        logger.info(f"📝 API response details log: {self.api_response_file}")
    
    def close_chunk_loggers(self):
        """Close chunk log files"""
        if hasattr(self, 'api_log') and self.api_log:
            self.api_log.close()
        if hasattr(self, 'db_log') and self.db_log:
            self.db_log.close()
        if hasattr(self, 'failed_log') and self.failed_log:
            self.failed_log.close()
        if hasattr(self, 'api_response_log') and self.api_response_log:
            self.api_response_log.close()
        if hasattr(self, 'fallback_failure_log') and self.fallback_failure_log:
            self.fallback_failure_log.close()

    def connect_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def close_db(self):
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("Database connection closed")

    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """
        Generate date ranges in chunks with overlap to ensure no data is missed.
        OVERLAP: Each chunk overlaps with the previous chunk by overlap_days to catch boundary records.
        """
        date_ranges = []
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()

        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end

            date_ranges.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))

            next_start = chunk_end - timedelta(days=overlap_days - 1)

            if chunk_end >= end:
                break

            current_date = next_start

        return date_ranges

    def fetch_accused_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        url = f"{API_CONFIG['base_url']}/accused"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching accused: {from_date} to {to_date} (Attempt {attempt + 1})")
                resp = requests.get(url, params=params, headers=headers, timeout=API_CONFIG['timeout'])
                if resp.status_code == 200:
                    data = resp.json()
                    self.stats['total_api_calls'] += 1
                    if data.get('status'):
                        rows = data.get('data') or []
                        if isinstance(rows, dict):
                            rows = [rows]
                        logger.info(f"✅ Fetched {len(rows)} accused for {from_date} to {to_date}")
                        
                        # Log API response details (accused_id, crime_id, person_id)
                        for row in rows:
                            accused_id = row.get('ACCUSED_ID', '')
                            crime_id = row.get('CRIME_ID', '')
                            person_id = row.get('PERSON_ID', '') or ''  # Log empty string if None
                            self.api_response_log.write(f"{accused_id}|{crime_id}|{person_id}\n")
                        self.api_response_log.flush()
                        
                        return rows
                    return []
                elif resp.status_code == 404:
                    return []
                else:
                    logger.warning(f"API {resp.status_code}, retrying...")
                    time.sleep(2 ** attempt)
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                time.sleep(2 ** attempt)
        logger.error(f"❌ Failed to fetch accused for {from_date} to {to_date}")
        return None

    def fetch_crime_by_id(self, crime_id: str) -> Optional[Dict]:
        """
        Fetch a single crime record by crime_id using the API endpoint.
        This is used when an accused record fails due to crime_not_found.
        
        Args:
            crime_id: The crime_id to fetch
            
        Returns:
            Crime dict, or None if fetch failed
        """
        url = f"{API_CONFIG['base_url']}/crimes/{crime_id}"
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching crime by crime_id: {crime_id} (Attempt {attempt + 1})")
                resp = requests.get(url, headers=headers, timeout=API_CONFIG['timeout'])
                
                if resp.status_code == 200:
                    data = resp.json()
                    self.stats['total_api_calls'] += 1
                    
                    if data.get('status'):
                        crime_data = data.get('data')
                        if crime_data:
                            logger.info(f"✅ Fetched crime by crime_id {crime_id}")
                            return crime_data
                        else:
                            logger.warning(f"⚠️  No crime data returned for crime_id {crime_id}")
                            return None
                    else:
                        logger.warning(f"⚠️  API returned status=false for crime_id {crime_id}")
                        return None
                
                elif resp.status_code == 404:
                    logger.warning(f"⚠️  No crime found for crime_id {crime_id} (404)")
                    return None
                
                else:
                    logger.warning(f"API returned status code {resp.status_code} for crime_id {crime_id}, retrying...")
                    time.sleep(2 ** attempt)
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout for crime_id {crime_id}, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error fetching crime by crime_id {crime_id}: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"crime_id {crime_id}: {str(e)}")
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch crime by crime_id {crime_id} after {API_CONFIG['max_retries']} attempts")
        return None
    
    def transform_crime(self, crime_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            crime_raw: Raw crime data from API
            
        Returns:
            Transformed crime dict ready for database
        """
        logger.trace(f"Transforming crime: CRIME_ID={crime_raw.get('CRIME_ID')}, FIR_NUM={crime_raw.get('FIR_NUM')}")
        transformed = {
            'crime_id': crime_raw.get('CRIME_ID'),
            'ps_code': crime_raw.get('PS_CODE'),
            'fir_num': crime_raw.get('FIR_NUM'),
            'fir_reg_num': crime_raw.get('FIR_REG_NUM'),
            'fir_type': crime_raw.get('FIR_TYPE'),
            'acts_sections': crime_raw.get('ACTS_SECTIONS'),
            'fir_date': crime_raw.get('FIR_DATE'),
            'case_status': crime_raw.get('CASE_STATUS'),
            'major_head': crime_raw.get('MAJOR_HEAD'),
            'minor_head': crime_raw.get('MINOR_HEAD'),
            'crime_type': crime_raw.get('CRIME_TYPE'),
            'io_name': crime_raw.get('IO_NAME'),
            'io_rank': crime_raw.get('IO_RANK'),
            'brief_facts': crime_raw.get('BRIEF_FACTS'),
            # Dates are always from API (never use CURRENT_TIMESTAMP)
            # If API doesn't provide dates, they will be NULL
            'date_created': crime_raw.get('DATE_CREATED') or None,
            'date_modified': crime_raw.get('DATE_MODIFIED') or None
        }
        logger.trace(f"Transformed crime: {json.dumps(transformed, indent=2, default=str)}")
        return transformed
    
    def insert_crime(self, crime: Dict) -> Tuple[bool, str]:
        """
        Insert or update single crime into database
        Simplified version for fallback crime insertion from accused ETL
        
        Args:
            crime: Transformed crime dict
            
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', or error reason
        """
        crime_id = crime.get('crime_id')
        if not crime_id:
            logger.warning(f"⚠️  Crime record missing CRIME_ID")
            return False, 'missing_crime_id'
        
        try:
            # Check if PS_CODE exists in hierarchy
            if crime.get('ps_code'):
                self.db_cursor.execute(f"SELECT 1 FROM {TABLE_CONFIG.get('hierarchy', 'hierarchy')} WHERE ps_code = %s", (crime['ps_code'],))
                if not self.db_cursor.fetchone():
                    logger.warning(f"⚠️  PS_CODE {crime['ps_code']} not found in hierarchy table for crime {crime_id}")
                    return False, 'ps_code_not_found'
            else:
                logger.warning(f"⚠️  Crime record missing PS_CODE for crime {crime_id}")
                return False, 'missing_ps_code'
            
            # Check if crime already exists
            self.db_cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
            exists = self.db_cursor.fetchone() is not None
            
            if exists:
                # Update existing crime (simple update, not smart update like etl_crimes.py)
                update_query = f"""
                    UPDATE {CRIMES_TABLE} SET
                        ps_code = %s, fir_num = %s, fir_reg_num = %s, fir_type = %s,
                        acts_sections = %s, fir_date = %s, case_status = %s, major_head = %s,
                        minor_head = %s, crime_type = %s, io_name = %s, io_rank = %s,
                        brief_facts = %s, date_created = %s, date_modified = %s
                    WHERE crime_id = %s
                """
                self.db_cursor.execute(update_query, (
                    crime['ps_code'], crime['fir_num'], crime['fir_reg_num'], crime['fir_type'],
                    crime['acts_sections'], crime['fir_date'], crime['case_status'],
                    crime['major_head'], crime['minor_head'], crime['crime_type'],
                    crime['io_name'], crime['io_rank'], crime['brief_facts'],
                    crime['date_created'], crime['date_modified'], crime_id
                ))
                logger.debug(f"Updated crime: {crime_id}")
                self.db_conn.commit()
                return True, 'updated'
            else:
                # Insert new crime
                insert_query = f"""
                    INSERT INTO {CRIMES_TABLE} (
                        crime_id, ps_code, fir_num, fir_reg_num, fir_type,
                        acts_sections, fir_date, case_status, major_head, minor_head,
                        crime_type, io_name, io_rank, brief_facts,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    crime['crime_id'], crime['ps_code'], crime['fir_num'], crime['fir_reg_num'],
                    crime['fir_type'], crime['acts_sections'], crime['fir_date'],
                    crime['case_status'], crime['major_head'], crime['minor_head'],
                    crime['crime_type'], crime['io_name'], crime['io_rank'],
                    crime['brief_facts'], crime['date_created'], crime['date_modified']
                ))
                logger.info(f"✅ Inserted crime: {crime_id} (from accused ETL fallback)")
                self.db_conn.commit()
                return True, 'inserted'
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            logger.warning(f"⚠️  Integrity error for crime {crime_id}: {e}")
            return False, 'integrity_error'
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error inserting crime {crime_id}: {e}")
            self.stats['errors'].append(f"Crime {crime_id}: {str(e)}")
            return False, 'error'
    
    def fetch_accused_by_crime_id(self, crime_id: str) -> Optional[List[Dict]]:
        """
        Fetch accused records by crime_id using the fallback API endpoint.
        This is used when an accused record fails due to crime_not_found.
        
        Args:
            crime_id: The crime_id to fetch accused for
            
        Returns:
            List of accused records, or None if fetch failed
        """
        url = f"{API_CONFIG['base_url']}/accused/{crime_id}"
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching accused by crime_id: {crime_id} (Attempt {attempt + 1})")
                resp = requests.get(url, headers=headers, timeout=API_CONFIG['timeout'])
                
                if resp.status_code == 200:
                    data = resp.json()
                    self.stats['total_api_calls'] += 1
                    
                    if data.get('status'):
                        rows = data.get('data') or []
                        if isinstance(rows, dict):
                            rows = [rows]
                        
                        if rows:
                            logger.info(f"✅ Fetched {len(rows)} accused by crime_id {crime_id}")
                            
                            # Log API response details (accused_id, crime_id, person_id)
                            for row in rows:
                                accused_id = row.get('ACCUSED_ID', '')
                                fetched_crime_id = row.get('CRIME_ID', '')
                                person_id = row.get('PERSON_ID', '') or ''
                                self.api_response_log.write(f"{accused_id}|{fetched_crime_id}|{person_id}\n")
                            self.api_response_log.flush()
                            
                            return rows
                        else:
                            logger.warning(f"⚠️  No accused data returned for crime_id {crime_id}")
                            return []
                    else:
                        logger.warning(f"⚠️  API returned status=false for crime_id {crime_id}")
                        return []
                
                elif resp.status_code == 404:
                    logger.warning(f"⚠️  No accused found for crime_id {crime_id} (404)")
                    return []
                
                else:
                    logger.warning(f"API returned status code {resp.status_code} for crime_id {crime_id}, retrying...")
                    time.sleep(2 ** attempt)
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout for crime_id {crime_id}, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error fetching accused by crime_id {crime_id}: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"crime_id {crime_id}: {str(e)}")
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch accused by crime_id {crime_id} after {API_CONFIG['max_retries']} attempts")
        return None

    def ensure_person_stub(self, person_id: str):
        if not person_id:
            return
        self.db_cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id,))
        if not self.db_cursor.fetchone():
            self.db_cursor.execute(
                f"INSERT INTO {PERSONS_TABLE} (person_id) VALUES (%s) ON CONFLICT (person_id) DO NOTHING",
                (person_id,)
            )
            self.stats['stub_persons_created'] += 1

    def accused_exists(self, accused_id: str) -> bool:
        """Check if accused exists by accused_id"""
        self.db_cursor.execute(f"SELECT 1 FROM {ACCUSED_TABLE} WHERE accused_id = %s", (accused_id,))
        return self.db_cursor.fetchone() is not None
    
    def accused_exists_by_crime_and_code(self, crime_id: str, accused_code: str) -> Optional[str]:
        """
        Check if accused exists by (crime_id, accused_code) combination
        Returns the accused_id if found, None otherwise
        NOTE: This method is kept for backward compatibility but is no longer used
        in the main insert logic since the unique constraint on (crime_id, accused_code) was removed.
        Multiple accused_id can now share the same (crime_id, accused_code) combination.
        """
        if not crime_id:
            return None
        
        # Normalize accused_code: None or empty string becomes empty string
        accused_code = accused_code or ''
        
        self.db_cursor.execute(
            f"SELECT accused_id FROM {ACCUSED_TABLE} WHERE crime_id = %s AND accused_code = %s",
            (crime_id, accused_code)
        )
        row = self.db_cursor.fetchone()
        return row[0] if row else None

    def transform_accused(self, row: Dict) -> Dict:
        """Transform API response to database format"""
        pf = row.get('PHYSICAL_FEATURES') or {}
        
        # Extract dates from API if available, otherwise None
        date_created = row.get('DATE_CREATED') or None
        date_modified = row.get('DATE_MODIFIED') or None
        
        # Normalize person_id: empty string or None becomes None (for NULL in database)
        person_id = row.get('PERSON_ID')
        if person_id == '' or person_id is None:
            person_id = None
        
        return {
            'accused_id': row.get('ACCUSED_ID'),
            'crime_id': row.get('CRIME_ID'),
            'person_id': person_id,
            'accused_code': row.get('ACCUSED_CODE'),
            'type': row.get('TYPE') or 'Accused',
            'seq_num': row.get('SEQ_NUM'),
            'is_ccl': bool(row.get('IS_CCL')),
            'beard': pf.get('BEARD'),
            'build': pf.get('BUILD'),
            'color': pf.get('COLOR'),
            'ear': pf.get('EAR'),
            'eyes': pf.get('EYES'),
            'face': pf.get('FACE'),
            'hair': pf.get('HAIR'),
            'height': pf.get('HEIGHT'),
            'leucoderma': pf.get('LEUCODERMA'),
            'mole': pf.get('MOLE'),
            'mustache': pf.get('MUSTACHE'),
            'nose': pf.get('NOSE'),
            'teeth': pf.get('TEETH'),
            'date_created': date_created,  # From API if available
            'date_modified': date_modified  # From API if available
        }
    
    def get_existing_accused(self, accused_id: str) -> Optional[Dict]:
        """Get existing accused record from database"""
        self.db_cursor.execute(f"""
            SELECT accused_id, crime_id, person_id, accused_code, type, seq_num, is_ccl,
                   beard, build, color, ear, eyes, face, hair, height,
                   leucoderma, mole, mustache, nose, teeth,
                   date_created, date_modified
            FROM {ACCUSED_TABLE}
            WHERE accused_id = %s
        """, (accused_id,))
        row = self.db_cursor.fetchone()
        if row:
            return {
                'accused_id': row[0],
                'crime_id': row[1],
                'person_id': row[2],
                'accused_code': row[3],
                'type': row[4],
                'seq_num': row[5],
                'is_ccl': row[6],
                'beard': row[7],
                'build': row[8],
                'color': row[9],
                'ear': row[10],
                'eyes': row[11],
                'face': row[12],
                'hair': row[13],
                'height': row[14],
                'leucoderma': row[15],
                'mole': row[16],
                'mustache': row[17],
                'nose': row[18],
                'teeth': row[19],
                'date_created': row[20],
                'date_modified': row[21]
            }
        return None
    
    def log_failed_record(self, accused: Dict, reason: str, error_details: str = ""):
        """Log a failed record to the failed records log file"""
        failed_info = {
            'accused_id': accused.get('accused_id'),
            'crime_id': accused.get('crime_id'),
            'person_id': accused.get('person_id'),
            'accused_code': accused.get('accused_code'),
            'reason': reason,
            'error_details': error_details,
            'timestamp': datetime.now().isoformat(),
            'accused_data': accused
        }
        
        self.failed_log.write(f"\n{'='*80}\n")
        self.failed_log.write(f"ACCUSED_ID: {accused.get('accused_id')}\n")
        self.failed_log.write(f"CRIME_ID: {accused.get('crime_id')}\n")
        self.failed_log.write(f"PERSON_ID: {accused.get('person_id')}\n")
        self.failed_log.write(f"ACCUSED_CODE: {accused.get('accused_code')}\n")
        self.failed_log.write(f"REASON: {reason}\n")
        if error_details:
            self.failed_log.write(f"ERROR: {error_details}\n")
        self.failed_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.failed_log.write(f"\nJSON Format:\n")
        self.failed_log.write(json.dumps(failed_info, indent=2, ensure_ascii=False, default=str))
        self.failed_log.write(f"\n")
        self.failed_log.flush()

    def _insert_accused_without_crime_check(self, accused: Dict, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Internal method to insert accused without checking if crime exists.
        Used for fallback accused records when we've already verified the crime exists.
        """
        accused_id = accused.get('accused_id')
        crime_id = accused.get('crime_id')
        person_id = accused.get('person_id')
        # Normalize: empty string becomes None (for NULL in database)
        if person_id == '':
            person_id = None
        accused['person_id'] = person_id
        
        if not accused_id:
            return False, 'missing_accused_id'
        
        if not crime_id:
            return False, 'missing_crime_id'
        
        try:
            # Check if person exists (create stub if needed) - only if person_id is provided
            if person_id:
                self.db_cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id,))
                person_exists = self.db_cursor.fetchone() is not None
                
                if not person_exists:
                    try:
                        self.db_cursor.execute(
                            f"INSERT INTO {PERSONS_TABLE} (person_id) VALUES (%s) ON CONFLICT (person_id) DO NOTHING",
                            (person_id,)
                        )
                        self.stats['stub_persons_created'] += 1
                        logger.trace(f"Created stub person: {person_id}")
                    except Exception as e:
                        return False, f'person_not_found: {str(e)}'
            
            # Determine dates: Priority 1) API dates, 2) Crime dates, 3) NULL
            date_created = accused.get('date_created')
            date_modified = accused.get('date_modified')
            
            if not date_created or not date_modified:
                self.db_cursor.execute(
                    f"SELECT date_created, date_modified FROM {CRIMES_TABLE} WHERE crime_id = %s",
                    (crime_id,)
                )
                crime_row = self.db_cursor.fetchone()
                if crime_row:
                    if not date_created:
                        date_created = crime_row[0]
                    if not date_modified:
                        date_modified = crime_row[1]
            
            accused['date_created'] = date_created
            accused['date_modified'] = date_modified
            
            accused_code = accused.get('accused_code') or ''
            
            # Check if accused already exists
            if self.accused_exists(accused_id):
                existing = self.get_existing_accused(accused_id)
                if existing:
                    # Smart update logic (same as main insert_accused)
                    update_fields = []
                    update_values = []
                    changes = []
                    
                    fields_to_check = [
                        ('crime_id', 'crime_id'), ('person_id', 'person_id'), ('accused_code', 'accused_code'),
                        ('type', 'type'), ('seq_num', 'seq_num'), ('is_ccl', 'is_ccl'),
                        ('beard', 'beard'), ('build', 'build'), ('color', 'color'), ('ear', 'ear'),
                        ('eyes', 'eyes'), ('face', 'face'), ('hair', 'hair'), ('height', 'height'),
                        ('leucoderma', 'leucoderma'), ('mole', 'mole'), ('mustache', 'mustache'),
                        ('nose', 'nose'), ('teeth', 'teeth'), ('date_created', 'date_created'),
                        ('date_modified', 'date_modified')
                    ]
                    
                    for new_key, db_key in fields_to_check:
                        new_val = accused.get(new_key)
                        existing_val = existing.get(new_key)
                        
                        if new_val != existing_val:
                            if existing_val is None and new_val is not None:
                                update_fields.append(f"{db_key} = %s")
                                update_values.append(new_val)
                                changes.append(f"{new_key}: NULL → {new_val}")
                            elif existing_val is not None and new_val is None:
                                # Keep existing value, don't update to NULL
                                pass
                            elif existing_val != new_val:
                                update_fields.append(f"{db_key} = %s")
                                update_values.append(new_val)
                                changes.append(f"{new_key}: {existing_val} → {new_val}")
                    
                    if update_fields:
                        update_values.append(accused_id)
                        self.db_cursor.execute(
                            f"UPDATE {ACCUSED_TABLE} SET {', '.join(update_fields)}, date_modified = %s WHERE accused_id = %s",
                            update_values + [date_modified, accused_id]
                        )
                        self.db_conn.commit()
                        self.stats['total_accused_updated'] += 1
                        logger.debug(f"Updated fallback accused: {accused_id} ({len(changes)} fields changed)")
                        return True, 'updated'
                    else:
                        self.stats['total_accused_no_change'] += 1
                        return True, 'no_change'
                else:
                    # Exists check returned True but couldn't fetch - treat as new insert
                    pass
            else:
                existing = None
            
            # Insert new accused
            if not existing:
                self.db_cursor.execute(
                    f"""
                    INSERT INTO {ACCUSED_TABLE} (
                        accused_id, crime_id, person_id, accused_code, type, seq_num, is_ccl,
                        beard, build, color, ear, eyes, face, hair, height,
                        leucoderma, mole, mustache, nose, teeth,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s
                    )
                    """,
                    (
                        accused_id, crime_id, person_id, accused_code,
                        accused.get('type'), accused.get('seq_num'), accused.get('is_ccl'),
                        accused.get('beard'), accused.get('build'), accused.get('color'),
                        accused.get('ear'), accused.get('eyes'), accused.get('face'),
                        accused.get('hair'), accused.get('height'), accused.get('leucoderma'),
                        accused.get('mole'), accused.get('mustache'), accused.get('nose'),
                        accused.get('teeth'), date_created, date_modified
                    )
                )
                self.db_conn.commit()
                self.stats['total_accused_inserted'] += 1
                logger.debug(f"Inserted fallback accused: {accused_id}")
                return True, 'inserted'
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error inserting fallback accused {accused_id}: {e}")
            return False, f'insert_error: {str(e)}'
    
    def insert_accused(self, accused: Dict, chunk_date_range: str = "") -> Tuple[bool, str]:
        """
        Insert or update single accused into database with smart update logic
        Dates priority: API dates > Crime dates > NULL (never use CURRENT_TIMESTAMP)
        
        Args:
            accused: Transformed accused dict
            chunk_date_range: Date range for chunk tracking
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'inserted', 'updated', 'no_change', or error reason
        """
        accused_id = accused.get('accused_id')
        crime_id = accused.get('crime_id')
        person_id = accused.get('person_id')
        # Normalize: empty string becomes None (for NULL in database)
        if person_id == '':
            person_id = None
        # Update the accused dict with normalized person_id
        accused['person_id'] = person_id
        
        if not accused_id:
            reason = 'missing_accused_id'
            error_details = "Accused record missing ACCUSED_ID"
            logger.warning(f"⚠️  {error_details}")
            self.stats['total_accused_failed'] += 1
            self.log_failed_record(accused, reason, error_details)
            return False, reason
        
        if not crime_id:
            reason = 'missing_crime_id'
            error_details = "Accused record missing CRIME_ID"
            logger.warning(f"⚠️  {error_details}")
            self.stats['total_accused_failed'] += 1
            self.stats['accused_without_crime'] += 1
            self.log_failed_record(accused, reason, error_details)
            return False, reason
        
        try:
            logger.trace(f"Processing accused: ACCUSED_ID={accused_id}, CRIME_ID={crime_id}, PERSON_ID={person_id or 'NULL'}")
            
            # Check if crime exists
            self.db_cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
            crime_exists = self.db_cursor.fetchone() is not None
            
            if not crime_exists:
                # STEP 1: Try to fetch and insert the crime first
                logger.info(f"🔄 CRIME_ID {crime_id} not found in database, trying to fetch crime from API...")
                crime_data = self.fetch_crime_by_id(crime_id)
                
                if crime_data:
                    # Transform and insert the crime
                    crime_transformed = self.transform_crime(crime_data)
                    crime_success, crime_operation = self.insert_crime(crime_transformed)
                    
                    if crime_success:
                        logger.info(f"✅ Successfully inserted crime {crime_id} from API (operation: {crime_operation})")
                        # Track crime insertions from accused ETL
                        if crime_operation == 'inserted':
                            self.stats['crimes_inserted_from_accused'] += 1
                        elif crime_operation == 'updated':
                            self.stats['crimes_updated_from_accused'] += 1
                        # Crime now exists, continue with normal flow
                        crime_exists = True
                    else:
                        logger.warning(f"⚠️  Failed to insert crime {crime_id}: {crime_operation}")
                        # Continue to fallback accused API as before
                else:
                    logger.warning(f"⚠️  Could not fetch crime {crime_id} from API, trying fallback accused API...")
                
                # STEP 2: If crime still doesn't exist, try fallback: fetch accused by crime_id from API
                if not crime_exists:
                    logger.info(f"🔄 CRIME_ID {crime_id} still not found, trying fallback accused API endpoint...")
                    fallback_accused = self.fetch_accused_by_crime_id(crime_id)
                    
                    if fallback_accused and len(fallback_accused) > 0:
                        # Found accused records via fallback API
                        logger.info(f"✅ Found {len(fallback_accused)} accused via fallback API for crime_id {crime_id}")
                        
                        # Check if crime exists now (maybe it was just inserted by another process)
                        self.db_cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
                        crime_exists_now = self.db_cursor.fetchone() is not None
                        
                        if crime_exists_now:
                            # Crime exists now, process the fallback accused records
                            success_count = 0
                            for fallback_row in fallback_accused:
                                # Transform the fallback accused data
                                fallback_accused_transformed = self.transform_accused(fallback_row)
                                fallback_accused_id = fallback_accused_transformed.get('accused_id')
                                
                                if fallback_accused_id:
                                    # Use helper method to insert without crime check (we already verified crime exists)
                                    result, operation = self._insert_accused_without_crime_check(fallback_accused_transformed, chunk_date_range)
                                    if result:
                                        success_count += 1
                                        logger.info(f"✅ Successfully inserted fallback accused {fallback_accused_id} for crime_id {crime_id}")
                                    else:
                                        logger.warning(f"⚠️  Failed to insert fallback accused {fallback_accused_id}: {operation}")
                                        self.fallback_failure_log.write(f"{fallback_accused_id}|{fallback_accused_transformed.get('person_id') or ''}|{crime_id}|insert_failed:{operation}\n")
                                        self.fallback_failure_log.flush()
                            
                            if success_count > 0:
                                logger.info(f"✅ Successfully processed {success_count}/{len(fallback_accused)} fallback accused records for crime_id {crime_id}")
                                # Continue with normal flow to process the original accused record - crime check will pass now
                            else:
                                # All fallback attempts failed even though crime exists
                                reason = 'crime_not_found_fallback_insert_failed'
                                error_details = f"CRIME_ID {crime_id} found after fallback but failed to insert {len(fallback_accused)} accused records"
                                logger.warning(f"⚠️  {error_details}")
                                self.stats['total_accused_failed'] += 1
                                self.stats['accused_without_crime'] += 1
                                self.log_failed_record(accused, reason, error_details)
                                self.fallback_failure_log.write(f"{accused_id}|{person_id or ''}|{crime_id}|fallback_insert_failed\n")
                                self.fallback_failure_log.flush()
                                return False, reason
                        else:
                            # Crime still doesn't exist - log all fallback accused to failure file
                            logger.warning(f"⚠️  Crime {crime_id} still not found after fallback API call")
                            for fallback_row in fallback_accused:
                                fallback_accused_transformed = self.transform_accused(fallback_row)
                                fallback_accused_id = fallback_accused_transformed.get('accused_id')
                                fallback_person_id = fallback_accused_transformed.get('person_id') or ''
                                self.fallback_failure_log.write(f"{fallback_accused_id}|{fallback_person_id}|{crime_id}|crime_still_not_found_after_fallback\n")
                            self.fallback_failure_log.flush()
                            
                            reason = 'crime_not_found_fallback_no_crime'
                            error_details = f"CRIME_ID {crime_id} not found and still not found after fallback API returned {len(fallback_accused)} records"
                            logger.warning(f"⚠️  {error_details}, skipping accused {accused_id}")
                            self.stats['total_accused_failed'] += 1
                            self.stats['accused_without_crime'] += 1
                            self.log_failed_record(accused, reason, error_details)
                            self.fallback_failure_log.write(f"{accused_id}|{person_id or ''}|{crime_id}|fallback_no_crime\n")
                            self.fallback_failure_log.flush()
                            return False, reason
                    else:
                        # Fallback API also failed or returned no data
                        reason = 'crime_not_found_fallback_no_data'
                        error_details = f"CRIME_ID {crime_id} not found in crimes table and fallback API returned no data"
                        logger.warning(f"⚠️  {error_details}, skipping accused {accused_id}")
                        self.stats['total_accused_failed'] += 1
                        self.stats['accused_without_crime'] += 1
                        self.log_failed_record(accused, reason, error_details)
                        # Log to fallback failure file
                        self.fallback_failure_log.write(f"{accused_id}|{person_id or ''}|{crime_id}|fallback_api_no_data\n")
                        self.fallback_failure_log.flush()
                        return False, reason
            
            # Check if person exists (create stub if needed) - only if person_id is provided
            if person_id:
                self.db_cursor.execute(f"SELECT 1 FROM {PERSONS_TABLE} WHERE person_id = %s", (person_id,))
                person_exists = self.db_cursor.fetchone() is not None
                
                if not person_exists:
                    # Try to create stub person
                    try:
                        self.db_cursor.execute(
                            f"INSERT INTO {PERSONS_TABLE} (person_id) VALUES (%s) ON CONFLICT (person_id) DO NOTHING",
                            (person_id,)
                        )
                        self.stats['stub_persons_created'] += 1
                        logger.trace(f"Created stub person: {person_id}")
                    except Exception as e:
                        reason = 'person_not_found'
                        error_details = f"PERSON_ID {person_id} not found and could not create stub: {str(e)}"
                        logger.warning(f"⚠️  {error_details}, skipping accused {accused_id}")
                        self.stats['total_accused_failed'] += 1
                        self.stats['accused_without_person'] += 1
                        self.log_failed_record(accused, reason, error_details)
                        return False, reason
            else:
                # person_id is NULL/empty - this is now allowed
                logger.trace(f"Processing accused with NULL person_id: {accused_id}")
            
            # Determine dates: Priority 1) API dates, 2) Crime dates, 3) NULL
            date_created = accused.get('date_created')
            date_modified = accused.get('date_modified')
            
            if not date_created or not date_modified:
                # Get crime dates as fallback
                self.db_cursor.execute(
                        f"SELECT date_created, date_modified FROM {CRIMES_TABLE} WHERE crime_id = %s",
                        (crime_id,)
                    )
                crime_row = self.db_cursor.fetchone()
                if crime_row:
                    if not date_created:
                        date_created = crime_row[0]
                    if not date_modified:
                        date_modified = crime_row[1]
            
            # Update accused dict with final dates
            accused['date_created'] = date_created
            accused['date_modified'] = date_modified
            
            # Normalize accused_code: None or empty string becomes empty string
            accused_code = accused.get('accused_code') or ''
            
            # Check if accused already exists by accused_id (primary key)
            # NOTE: We no longer check by (crime_id, accused_code) since that constraint is removed
            # Multiple accused_id can now share the same (crime_id, accused_code) combination
            if self.accused_exists(accused_id):
                # Check if accused already exists by accused_id
                # Get existing record to compare
                existing = self.get_existing_accused(accused_id)
                if not existing:
                    logger.warning(f"⚠️  ACCUSED_ID {accused_id} exists check returned True but fetch returned None")
                    existing = None
            else:
                existing = None
            
            if existing:
                    # Smart update: only update fields that need updating
                    # Rules:
                    # 1. If existing is NULL and new is not NULL → update
                    # 2. If existing is not NULL and new is NULL → keep existing (don't update to NULL)
                    # 3. If both are not NULL and different → update
                    # 4. If both are not NULL and same → skip update (no change needed)
                    # Special: date_created and date_modified always from API/crime (even if NULL)
                    
                    update_fields = []
                    update_values = []
                    changes = []
                    
                    # Define all fields to check (excluding accused_id which is the key)
                    fields_to_check = [
                        ('crime_id', 'crime_id'),
                        ('person_id', 'person_id'),
                        ('accused_code', 'accused_code'),
                        ('type', 'type'),
                        ('seq_num', 'seq_num'),
                        ('is_ccl', 'is_ccl'),
                        ('beard', 'beard'),
                        ('build', 'build'),
                        ('color', 'color'),
                        ('ear', 'ear'),
                        ('eyes', 'eyes'),
                        ('face', 'face'),
                        ('hair', 'hair'),
                        ('height', 'height'),
                        ('leucoderma', 'leucoderma'),
                        ('mole', 'mole'),
                        ('mustache', 'mustache'),
                        ('nose', 'nose'),
                        ('teeth', 'teeth'),
                        ('date_created', 'date_created'),  # Always from API/crime
                        ('date_modified', 'date_modified')  # Always from API/crime
                    ]
                    
                    for db_field, accused_field in fields_to_check:
                        existing_val = existing.get(db_field)
                        new_val = accused.get(accused_field)
                        
                        # Special handling for date fields - always use API/crime value
                        if db_field in ('date_created', 'date_modified'):
                            # Always update date fields from API/crime (even if NULL)
                            if existing_val != new_val:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: {existing_val} → {new_val}")
                                logger.trace(f"  Will update {db_field}: {existing_val} → {new_val} (API/crime date)")
                        else:
                            # Rule 1: Existing is NULL, new is not NULL → update
                            if existing_val is None and new_val is not None:
                                update_fields.append(f"{db_field} = %s")
                                update_values.append(new_val)
                                changes.append(f"{db_field}: NULL → {new_val}")
                                logger.trace(f"  Will update {db_field}: NULL → {new_val}")
                            
                            # Rule 2: Existing is not NULL, new is NULL → keep existing (skip)
                            elif existing_val is not None and new_val is None:
                                logger.trace(f"  Will keep existing {db_field}: {existing_val} (new value is NULL)")
                                # Don't add to update - preserve existing value
                            
                            # Rule 3 & 4: Both are not NULL
                            elif existing_val is not None and new_val is not None:
                                # Rule 3: Different → update
                                if existing_val != new_val:
                                    update_fields.append(f"{db_field} = %s")
                                    update_values.append(new_val)
                                    changes.append(f"{db_field}: {existing_val} → {new_val}")
                                    logger.trace(f"  Will update {db_field}: {existing_val} → {new_val}")
                                # Rule 4: Same → skip (no change)
                                else:
                                    logger.trace(f"  No change for {db_field}: {existing_val}")
                            
                            # Both are NULL → no update needed
                            else:
                                logger.trace(f"  Both NULL for {db_field}, no update")
                    
                    # Only update if there are changes
                    if update_fields:
                        update_query = f"""
                            UPDATE {ACCUSED_TABLE} SET
                                {', '.join(update_fields)}
                            WHERE accused_id = %s
                        """
                        update_values.append(accused_id)
                        self.db_cursor.execute(update_query, tuple(update_values))
                        self.stats['total_accused_updated'] += 1
                        logger.debug(f"Updated accused: {accused_id} ({len(changes)} fields changed)")
                        logger.trace(f"Changes: {', '.join(changes)}")
                        self.db_conn.commit()
                        logger.trace(f"Transaction committed for updated ACCUSED_ID: {accused_id}")
                        return True, 'updated'
                    else:
                        # No changes needed
                        self.stats['total_accused_no_change'] += 1
                        logger.trace(f"No changes needed for ACCUSED_ID: {accused_id} (all fields match or preserved)")
                        return True, 'no_change'
            else:
                # Insert new accused (no existing record found)
                logger.trace(f"Inserting new accused: {accused_id}")
                
                # Use ON CONFLICT to handle duplicate accused_id (primary key) gracefully
                # If conflict occurs, update the existing record instead
                # NOTE: Multiple accused_id can now share the same (crime_id, accused_code) since that constraint is removed
                insert_query = f"""
                    INSERT INTO {ACCUSED_TABLE} (
                        accused_id, crime_id, person_id, accused_code, type, seq_num, is_ccl,
                        beard, build, color, ear, eyes, face, hair, height,
                        leucoderma, mole, mustache, nose, teeth,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (accused_id) 
                    DO UPDATE SET
                        crime_id = EXCLUDED.crime_id,
                        person_id = EXCLUDED.person_id,
                        accused_code = EXCLUDED.accused_code,
                        type = EXCLUDED.type,
                        seq_num = EXCLUDED.seq_num,
                        is_ccl = EXCLUDED.is_ccl,
                        beard = EXCLUDED.beard,
                        build = EXCLUDED.build,
                        color = EXCLUDED.color,
                        ear = EXCLUDED.ear,
                        eyes = EXCLUDED.eyes,
                        face = EXCLUDED.face,
                        hair = EXCLUDED.hair,
                        height = EXCLUDED.height,
                        leucoderma = EXCLUDED.leucoderma,
                        mole = EXCLUDED.mole,
                        mustache = EXCLUDED.mustache,
                        nose = EXCLUDED.nose,
                        teeth = EXCLUDED.teeth,
                        date_created = EXCLUDED.date_created,
                        date_modified = EXCLUDED.date_modified
                """
                
                try:
                    # Check if record exists before insert to determine if it's insert or update
                    existing_before = self.accused_exists(accused_id)
                    
                    self.db_cursor.execute(insert_query, (
                        accused['accused_id'],
                        accused['crime_id'],
                        accused['person_id'],
                        accused['accused_code'],
                        accused['type'],
                        accused['seq_num'],
                        accused['is_ccl'],
                        accused['beard'],
                        accused['build'],
                        accused['color'],
                        accused['ear'],
                        accused['eyes'],
                        accused['face'],
                        accused['hair'],
                        accused['height'],
                        accused['leucoderma'],
                        accused['mole'],
                        accused['mustache'],
                        accused['nose'],
                        accused['teeth'],
                        accused['date_created'],  # From API/crime (or NULL)
                        accused['date_modified']  # From API/crime (or NULL)
                    ))
                    
                    # Check if it was an insert or update
                    if self.db_cursor.rowcount == 0:
                        # No rows affected - might be duplicate or no change
                        if existing_before:
                            self.stats['total_accused_no_change'] += 1
                            logger.trace(f"No change for ACCUSED_ID: {accused_id} (already exists with same data)")
                            self.db_conn.commit()
                            return True, 'no_change'
                        else:
                            # This shouldn't happen, but handle it
                            logger.warning(f"⚠️  Row count is 0 but accused_id {accused_id} doesn't exist")
                            self.db_conn.commit()
                            return False, 'insert_failed'
                    else:
                        # Row was inserted or updated
                        if existing_before:
                            # Record existed before, so this was an update via ON CONFLICT
                            self.stats['total_accused_updated'] += 1
                            logger.debug(f"Updated accused via ON CONFLICT: {accused_id}")
                        else:
                            # Record didn't exist before, so this was an insert
                            self.stats['total_accused_inserted'] += 1
                            logger.debug(f"Inserted accused: {accused_id}")
                    
                    logger.trace(f"Insert/Update query executed for ACCUSED_ID: {accused_id}")
                    self.db_conn.commit()
                    logger.trace(f"Transaction committed for ACCUSED_ID: {accused_id}")
                    return True, 'inserted' if not existing_before else 'updated'
                    
                except psycopg2.IntegrityError as e:
                    # Still might get integrity error for accused_id primary key
                    if 'accused_id' in str(e).lower():
                        # accused_id already exists - treat as update
                        logger.warning(f"⚠️  ACCUSED_ID {accused_id} already exists, treating as update")
                        self.db_conn.rollback()
                        # Fall through to update logic below
                        existing = self.get_existing_accused(accused_id)
                        if existing:
                            # Use the smart update logic
                            # (This will be handled by the update logic above)
                            raise  # Re-raise to be caught by outer exception handler
                    else:
                        raise  # Re-raise other integrity errors
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            reason = 'integrity_error'
            error_details = str(e)
            logger.warning(f"⚠️  Integrity error for accused {accused_id}: {e}")
            self.stats['total_accused_failed'] += 1
            self.log_failed_record(accused, reason, error_details)
            return False, reason
        except Exception as e:
            self.db_conn.rollback()
            reason = 'error'
            error_details = str(e)
            logger.error(f"❌ Error inserting accused {accused_id}: {e}")
            self.stats['total_accused_failed'] += 1
            self.stats['errors'].append(f"Accused {accused_id}: {str(e)}")
            self.log_failed_record(accused, reason, error_details)
            return False, reason

    def log_api_chunk(self, from_date: str, to_date: str, accused_list: List[Dict], error: Optional[str] = None):
        """Log API response for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'total_fetched': len(accused_list),
            'accused_ids': [a.get('ACCUSED_ID') for a in accused_list],
            'error': error
        }
        
        self.api_log.write(f"\n{'='*80}\n")
        self.api_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.api_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.api_log.write(f"{'-'*80}\n")
        
        if error:
            self.api_log.write(f"ERROR: {error}\n")
        else:
            self.api_log.write(f"Total Fetched from API: {len(accused_list)}\n")
            self.api_log.write(f"\nACCUSED_IDs:\n")
            for i, accused in enumerate(accused_list, 1):
                self.api_log.write(f"  {i}. {accused.get('ACCUSED_ID')} (CRIME_ID: {accused.get('CRIME_ID')}, PERSON_ID: {accused.get('PERSON_ID')})\n")
        
        # Also write JSON format for easy parsing
        self.api_log.write(f"\nJSON Format:\n")
        self.api_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False, default=str))
        self.api_log.write(f"\n")
        self.api_log.flush()
    
    def log_db_chunk(self, from_date: str, to_date: str, total_fetched: int,
                    inserted_ids: List[str], updated_ids: List[str], no_change_ids: List[str],
                    failed_ids: List[str], failed_reasons: Dict, error: Optional[str] = None):
        """Log database operations for a chunk"""
        chunk_info = {
            'chunk': f"{from_date} to {to_date}",
            'timestamp': datetime.now().isoformat(),
            'total_fetched': total_fetched,
            'inserted_count': len(inserted_ids),
            'inserted_ids': inserted_ids,
            'updated_count': len(updated_ids),
            'updated_ids': updated_ids,
            'no_change_count': len(no_change_ids),
            'no_change_ids': no_change_ids,
            'failed_count': len(failed_ids),
            'failed_ids': failed_ids,
            'failed_reasons': failed_reasons,
            'error': error
        }
        
        self.db_log.write(f"\n{'='*80}\n")
        self.db_log.write(f"CHUNK: {from_date} to {to_date}\n")
        self.db_log.write(f"Timestamp: {datetime.now().isoformat()}\n")
        self.db_log.write(f"{'-'*80}\n")
        
        if error:
            self.db_log.write(f"ERROR: {error}\n")
        else:
            self.db_log.write(f"Total Fetched from API: {total_fetched}\n")
            self.db_log.write(f"\nINSERTED: {len(inserted_ids)}\n")
            for i, accused_id in enumerate(inserted_ids, 1):
                self.db_log.write(f"  {i}. {accused_id}\n")
            
            self.db_log.write(f"\nUPDATED: {len(updated_ids)}\n")
            for i, accused_id in enumerate(updated_ids, 1):
                self.db_log.write(f"  {i}. {accused_id}\n")
            
            self.db_log.write(f"\nNO CHANGE: {len(no_change_ids)}\n")
            for i, accused_id in enumerate(no_change_ids, 1):
                self.db_log.write(f"  {i}. {accused_id}\n")
            
            self.db_log.write(f"\nFAILED: {len(failed_ids)}\n")
            if failed_reasons:
                for reason, ids in failed_reasons.items():
                    self.db_log.write(f"  Reason: {reason} ({len(ids)})\n")
                    for i, accused_id in enumerate(ids[:20], 1):  # Show first 20
                        self.db_log.write(f"    {i}. {accused_id}\n")
                    if len(ids) > 20:
                        self.db_log.write(f"    ... and {len(ids) - 20} more\n")
            
            # Also write JSON format for easy parsing
            self.db_log.write(f"\nJSON Format:\n")
            self.db_log.write(json.dumps(chunk_info, indent=2, ensure_ascii=False))
            self.db_log.write(f"\n")
        
        self.db_log.flush()

    def process_date_range(self, from_date: str, to_date: str):
        """Process accused for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch accused from API
        accused_raw = self.fetch_accused_api(from_date, to_date)
        
        if accused_raw is None:
            logger.error(f"❌ Failed to fetch accused for {chunk_range}")
            self.log_api_chunk(from_date, to_date, [], error="API fetch failed")
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], {}, error="API fetch failed")
            return
        
        if not accused_raw:
            logger.info(f"ℹ️  No accused found for {chunk_range}")
            self.log_api_chunk(from_date, to_date, [])
            self.log_db_chunk(from_date, to_date, 0, [], [], [], [], {}, error="No accused in API response")
            return
        
        # Log API response
        self.log_api_chunk(from_date, to_date, accused_raw)
        
        # Transform and insert each accused
        self.stats['total_accused_fetched'] += len(accused_raw)
        logger.trace(f"Processing {len(accused_raw)} accused for chunk {chunk_range}")
        
        # Track operations for this chunk
        inserted_ids = []
        updated_ids = []
        no_change_ids = []
        failed_ids = []
        failed_reasons = {}
        duplicates_in_chunk = []
        
        # Track accused_ids seen in this chunk to detect duplicates (for reporting only, not skipping)
        seen_accused_ids = {}
        accused_id_occurrences = {}  # Track how many times each accused_id appears
        
        logger.trace(f"Starting to process records for chunk: {chunk_range}")
        for idx, accused_raw_row in enumerate(accused_raw, 1):
            logger.trace(f"Processing record {idx}/{len(accused_raw)}: {accused_raw_row.get('ACCUSED_ID')}")
            accused = self.transform_accused(accused_raw_row)
            accused_id = accused.get('accused_id')
            
            if not accused_id:
                logger.warning(f"⚠️  Accused missing ACCUSED_ID, skipping")
                self.stats['total_accused_failed'] += 1
                failed_ids.append(None)
                reason = 'missing_accused_id'
                if reason not in failed_reasons:
                    failed_reasons[reason] = []
                failed_reasons[reason].append(None)
                continue
            
            # Track occurrences for duplicate reporting (but don't skip - process all)
            if accused_id in seen_accused_ids:
                # This is a duplicate occurrence - track it but still process
                occurrence_count = accused_id_occurrences.get(accused_id, 1) + 1
                accused_id_occurrences[accused_id] = occurrence_count
                
                duplicates_in_chunk.append({
                    'accused_id': accused_id,
                    'crime_id': accused.get('crime_id'),
                    'person_id': accused.get('person_id'),
                    'occurrence': occurrence_count,
                    'first_seen_in': seen_accused_ids[accused_id],
                    'duplicate_in': chunk_range
                })
                self.stats['total_duplicates'] += 1
                logger.info(f"⚠️  Duplicate ACCUSED_ID {accused_id} found in chunk {chunk_range} (occurrence #{occurrence_count}) - Will process to update record")
                logger.trace(f"Duplicate details - First seen: {seen_accused_ids[accused_id]}, Current occurrence: {occurrence_count}")
            else:
                seen_accused_ids[accused_id] = chunk_range
                accused_id_occurrences[accused_id] = 1
                logger.trace(f"New ACCUSED_ID seen: {accused_id} in chunk {chunk_range}")
            
            # IMPORTANT: Process ALL records, even duplicates
            # If same accused_id appears multiple times, each occurrence might have updated data
            # The smart update logic will handle whether to actually update or not
            success, operation = self.insert_accused(accused, chunk_range)
            logger.trace(f"Operation result for ACCUSED_ID {accused.get('accused_id')}: success={success}, operation={operation}")
            
            if success:
                if operation == 'inserted':
                    # Only add to list if first occurrence (to avoid duplicate entries in log)
                    if accused_id not in inserted_ids:
                        inserted_ids.append(accused_id)
                    logger.trace(f"Added to inserted list: {accused_id}")
                elif operation == 'updated':
                    # Track all updates (even if same accused_id updated multiple times)
                    updated_ids.append(accused_id)
                    logger.trace(f"Added to updated list: {accused_id} (occurrence #{accused_id_occurrences.get(accused_id, 1)})")
                elif operation == 'no_change':
                    # Only add to list if first occurrence
                    if accused_id not in no_change_ids:
                        no_change_ids.append(accused_id)
                    logger.trace(f"Added to no_change list: {accused_id}")
            else:
                failed_ids.append(accused_id)
                if operation not in failed_reasons:
                    failed_reasons[operation] = []
                failed_reasons[operation].append(accused_id)
                logger.trace(f"Added to failed list: {accused_id}, reason: {operation}")
        
        # Log duplicates for this chunk (for reporting, but they were all processed)
        if duplicates_in_chunk:
            logger.info(f"📊 Found {len(duplicates_in_chunk)} duplicate occurrences in chunk {chunk_range} - All were processed for potential updates")
            logger.trace(f"Duplicate details: {duplicates_in_chunk}")
        
        # Log database operations for this chunk
        logger.trace(f"Chunk summary - Inserted: {len(inserted_ids)}, Updated: {len(updated_ids)}, No Change: {len(no_change_ids)}, Failed: {len(failed_ids)}, Duplicates: {len(duplicates_in_chunk)}")
        self.log_db_chunk(from_date, to_date, len(accused_raw), inserted_ids, updated_ids, 
                         no_change_ids, failed_ids, failed_reasons)
        
        logger.info(f"✅ Completed: {chunk_range} - Inserted: {len(inserted_ids)}, Updated: {len(updated_ids)}, No Change: {len(no_change_ids)}, Failed: {len(failed_ids)}, Duplicates: {len(duplicates_in_chunk)}")
        logger.trace(f"Chunk processing complete for {chunk_range}")

    def write_log_summaries(self):
        """Write summary sections to log files"""
        # API log summary
        self.api_log.write(f"\n\n{'='*80}\n")
        self.api_log.write(f"SUMMARY\n")
        self.api_log.write(f"{'='*80}\n")
        self.api_log.write(f"Total API Calls: {self.stats['total_api_calls']}\n")
        self.api_log.write(f"Total Accused Fetched: {self.stats['total_accused_fetched']}\n")
        self.api_log.write(f"Failed API Calls: {self.stats['failed_api_calls']}\n")
        self.api_log.write(f"Expected Total from API Team: 22423 (insert + update records)\n")
        
        # DB log summary
        self.db_log.write(f"\n\n{'='*80}\n")
        self.db_log.write(f"SUMMARY\n")
        self.db_log.write(f"{'='*80}\n")
        self.db_log.write(f"Total Accused Fetched from API: {self.stats['total_accused_fetched']}\n")
        self.db_log.write(f"Total Accused Inserted (New): {self.stats['total_accused_inserted']}\n")
        self.db_log.write(f"Total Accused Updated (Existing): {self.stats['total_accused_updated']}\n")
        self.db_log.write(f"Total Accused No Change: {self.stats['total_accused_no_change']}\n")
        self.db_log.write(f"Total Accused Failed: {self.stats['total_accused_failed']}\n")
        self.db_log.write(f"Total Operations (Inserted + Updated + No Change): {self.stats['total_accused_inserted'] + self.stats['total_accused_updated'] + self.stats['total_accused_no_change']}\n")
        if self.stats['total_accused_fetched'] > 0:
            coverage = ((self.stats['total_accused_inserted'] + self.stats['total_accused_updated'] + self.stats['total_accused_no_change']) / self.stats['total_accused_fetched']) * 100
            self.db_log.write(f"Coverage: {coverage:.2f}%\n")
        self.db_log.write(f"\nCrimes Inserted/Updated (from accused ETL fallback):\n")
        self.db_log.write(f"  Crimes Inserted: {self.stats['crimes_inserted_from_accused']}\n")
        self.db_log.write(f"  Crimes Updated: {self.stats['crimes_updated_from_accused']}\n")
        self.db_log.write(f"Errors: {len(self.stats['errors'])}\n")
        
        # Failed records log summary
        self.failed_log.write(f"\n\n{'='*80}\n")
        self.failed_log.write(f"SUMMARY\n")
        self.failed_log.write(f"{'='*80}\n")
        self.failed_log.write(f"Total Failed Records: {self.stats['total_accused_failed']}\n")
        self.failed_log.write(f"Note: Failed records are those that could not be inserted or updated\n")
        self.failed_log.write(f"Check individual entries above for specific reasons\n")
        self.failed_log.write(f"\nFailure Reasons:\n")
        self.failed_log.write(f"  - Crime Not Found: {self.stats['accused_without_crime']}\n")
        self.failed_log.write(f"  - Person Not Found: {self.stats['accused_without_person']}\n")

    def run(self):
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Accused API")
        logger.info("=" * 80)
        logger.info(f"Date Range: {ETL_CONFIG['start_date']} to {ETL_CONFIG['end_date']}")
        overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
        logger.info(f"Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) to ensure no data loss)")
        logger.info("=" * 80)

        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False
        
        try:
            ranges = self.generate_date_ranges(
                ETL_CONFIG['start_date'], 
                ETL_CONFIG['end_date'], 
                ETL_CONFIG['chunk_days'],
                ETL_CONFIG.get('chunk_overlap_days', 1)  # Default to 1 day overlap for safety
            )
            logger.info(f"📊 Total date ranges to process: {len(ranges)}")
            logger.info(f"ℹ️  Expected Total from API Team: 22423 (insert + update records)")
            logger.info("")
            
            for fd, td in tqdm(ranges, desc="Processing date ranges", unit="range"):
                self.process_date_range(fd, td)
                time.sleep(1)

            # Get database counts
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {ACCUSED_TABLE}")
            db_accused_count = self.db_cursor.fetchone()[0]
            
            # Store for summary
            self.stats['db_total_count'] = db_accused_count

            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 ETL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"Total API Calls:       {self.stats['total_api_calls']}")
            logger.info(f"Failed API Calls:      {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM API:")
            logger.info(f"  Total Accused Fetched: {self.stats['total_accused_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Accused Inserted:      {self.stats['total_accused_inserted']}")
            logger.info(f"  Accused Updated:       {self.stats['total_accused_updated']}")
            logger.info(f"  Accused No Change:     {self.stats['total_accused_no_change']}")
            logger.info(f"  Accused Failed:        {self.stats['total_accused_failed']}")
            logger.info(f"    - Crime Not Found:    {self.stats['accused_without_crime']}")
            logger.info(f"    - Person Not Found:   {self.stats['accused_without_person']}")
            logger.info(f"  Total in DB:           {db_accused_count}")
            logger.info(f"")
            logger.info(f"🔄 CRIMES INSERTED/UPDATED (from accused ETL fallback):")
            logger.info(f"  Crimes Inserted:       {self.stats['crimes_inserted_from_accused']}")
            logger.info(f"  Crimes Updated:        {self.stats['crimes_updated_from_accused']}")
            logger.info(f"")
            logger.info(f"🔄 DUPLICATES:")
            logger.info(f"  Total Duplicate Occurrences (Processed): {self.stats['total_duplicates']}")
            logger.info(f"  Note: All duplicates are processed to allow updates")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_accused_fetched'] > 0:
                coverage = ((self.stats['total_accused_inserted'] + self.stats['total_accused_updated'] + self.stats['total_accused_no_change']) / self.stats['total_accused_fetched']) * 100
                logger.info(f"  API → DB Coverage:    {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"📈 SUMMARY:")
            logger.info(f"  Total from API:        {self.stats['total_accused_fetched']}")
            logger.info(f"  Inserted + Updated:    {self.stats['total_accused_inserted'] + self.stats['total_accused_updated']}")
            logger.info(f"  Duplicate Occurrences: {self.stats['total_duplicates']} (all processed)")
            logger.info(f"  Failed:               {self.stats['total_accused_failed']}")
            logger.info(f"")
            logger.info(f"💡 NOTE:")
            logger.info(f"  - Same accused_id can appear multiple times in API response")
            logger.info(f"  - Each occurrence is processed to capture any data updates")
            logger.info(f"  - Smart update logic ensures only changed fields are updated")
            logger.info(f"")
            logger.info(f"Errors:                {len(self.stats['errors'])}")
            logger.info("=" * 80)
            
            if self.stats['errors']:
                logger.warning("⚠️  Errors encountered:")
                for error in self.stats['errors'][:10]:  # Show first 10 errors
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more")
            
            # Write summary to log files
            self.write_log_summaries()
            
            logger.info("✅ ETL Pipeline completed successfully!")
            logger.info(f"📝 API chunk log saved to: {self.api_log_file}")
            logger.info(f"📝 DB chunk log saved to: {self.db_log_file}")
            logger.info(f"📝 Failed records log saved to: {self.failed_log_file}")
            logger.info(f"📝 API response details log saved to: {self.api_response_file}")
            logger.info(f"📝 Fallback failures log saved to: {self.fallback_failure_file}")
            return True
        except KeyboardInterrupt:
            logger.warning("\n⚠️  ETL interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ ETL failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close_chunk_loggers()
            self.close_db()


def main():
    etl = AccusedETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()




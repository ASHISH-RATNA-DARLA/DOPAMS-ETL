#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - FIR_COPY Migration Script
One-time migration to backfill fir_copy column for existing crimes
Only updates fir_copy column, does not touch other fields
"""

import sys
import time
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import logging
import colorlog
from typing import List, Dict, Optional, Tuple
import json

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# Setup colored logging
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    LOG_CONFIG['format'],
    datefmt=LOG_CONFIG['date_format'],
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
))
logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(LOG_CONFIG['level'].upper())

# Target tables
CRIMES_TABLE = TABLE_CONFIG.get('crimes', 'crimes')

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

def parse_iso_date(iso_date_str: str) -> datetime:
    """
    Parse ISO 8601 date string to datetime object
    Supports formats:
    - YYYY-MM-DDTHH:MM:SS+TZ:TZ (e.g., '2022-10-01T00:00:00+05:30')
    - YYYY-MM-DD (e.g., '2022-10-01') - defaults to 00:00:00 IST
    """
    try:
        if 'T' in iso_date_str:
            return datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(iso_date_str, '%Y-%m-%d')
            return dt.replace(tzinfo=IST_OFFSET)
    except ValueError:
        dt = datetime.strptime(iso_date_str.split('T')[0], '%Y-%m-%d')
        return dt.replace(tzinfo=IST_OFFSET)

def iso_to_date_only(iso_date_str: str) -> str:
    """Extract date part from ISO 8601 format string"""
    if 'T' in iso_date_str:
        return iso_date_str.split('T')[0]
    return iso_date_str

def get_yesterday_end_ist() -> str:
    """Get yesterday's date at 23:59:59 in IST (UTC+05:30) as ISO format string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    return yesterday_end.isoformat()

class FIRCopyMigration:
    """Migration script to backfill fir_copy column"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_crimes_fetched': 0,
            'total_crimes_updated': 0,
            'total_crimes_no_change': 0,  # fir_copy already matches
            'total_crimes_not_found': 0,  # crime_id from API not in DB
            'total_crimes_failed': 0,
            'failed_api_calls': 0,
            'errors': []
        }
    
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_cursor = self.db_conn.cursor()
            logger.info(f"✅ Connected to database: {DB_CONFIG['database']}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()
        logger.info("Database connection closed")
    
    def check_fir_copy_column(self) -> bool:
        """Check if fir_copy column exists in crimes table"""
        try:
            self.db_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'fir_copy'
            """, (CRIMES_TABLE,))
            exists = self.db_cursor.fetchone() is not None
            if not exists:
                logger.error(f"❌ Column 'fir_copy' does not exist in {CRIMES_TABLE}")
                logger.error("   Please run: ALTER TABLE crimes ADD COLUMN fir_copy VARCHAR(50);")
            return exists
        except Exception as e:
            logger.error(f"❌ Error checking column: {e}")
            return False
    
    def get_effective_start_date(self) -> str:
        """
        Get effective start date for migration:
        - If table is empty: return 2022-01-01T00:00:00+05:30
        - If table has data: return max(date_created, date_modified) from table
        """
        try:
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {CRIMES_TABLE}")
            count = self.db_cursor.fetchone()[0]
            
            if count == 0:
                logger.info("📊 Table is empty, starting from 2022-01-01")
                return '2022-01-01T00:00:00+05:30'
            
            MIN_START_DATE = '2022-01-01T00:00:00+05:30'
            min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
            
            self.db_cursor.execute(f"""
                SELECT GREATEST(
                    COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                    COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                ) as max_date
                FROM {CRIMES_TABLE}
            """)
            result = self.db_cursor.fetchone()
            if result and result[0]:
                max_date = result[0]
                if isinstance(max_date, datetime):
                    if max_date.tzinfo is None:
                        max_date = max_date.replace(tzinfo=IST_OFFSET)
                    else:
                        max_date = max_date.astimezone(IST_OFFSET)
                    
                    if max_date < min_start_dt:
                        logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                        return MIN_START_DATE
                    
                    logger.info(f"📊 Table has data, starting from: {max_date.isoformat()}")
                    return max_date.isoformat()
            
            logger.warning("⚠️  Could not determine max date, using 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
    
    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """Generate date ranges in chunks with overlap"""
        date_ranges = []
        current_date = parse_iso_date(start_date)
        end = parse_iso_date(end_date)
        
        current_date_only = current_date.date()
        end_date_only = end.date()
        
        while current_date_only <= end_date_only:
            chunk_end_date = current_date_only + timedelta(days=chunk_days - 1)
            if chunk_end_date > end_date_only:
                chunk_end_date = end_date_only
            
            date_ranges.append((
                current_date_only.strftime('%Y-%m-%d'),
                chunk_end_date.strftime('%Y-%m-%d')
            ))
            
            next_start = chunk_end_date - timedelta(days=overlap_days - 1)
            
            if chunk_end_date >= end_date_only:
                break
            
            current_date_only = next_start
        
        return date_ranges
    
    def fetch_crimes_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """Fetch crimes from API for given date range"""
        url = f"{API_CONFIG['base_url']}/crimes"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching crimes: {from_date} to {to_date} (Attempt {attempt + 1})")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=API_CONFIG['timeout']
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.stats['total_api_calls'] += 1
                    
                    if data.get('status'):
                        crime_data = data.get('data')
                        if crime_data:
                            if isinstance(crime_data, dict):
                                crime_data = [crime_data]
                            logger.info(f"✅ Fetched {len(crime_data)} crimes for {from_date} to {to_date}")
                            return crime_data
                        else:
                            logger.warning(f"⚠️  No crimes found for {from_date} to {to_date}")
                            return []
                    else:
                        logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}")
                        return []
                
                elif response.status_code == 404:
                    logger.warning(f"⚠️  No data found for {from_date} to {to_date}")
                    return []
                
                else:
                    logger.warning(f"API returned status code {response.status_code}, retrying...")
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
        
        logger.error(f"❌ Failed to fetch crimes for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        return None
    
    def update_fir_copy(self, crime_id: str, fir_copy: Optional[str]) -> Tuple[bool, str]:
        """
        Update only fir_copy column for a crime
        
        Returns:
            Tuple of (success: bool, operation: str) where operation is 'updated', 'no_change', 'not_found', or 'error'
        """
        if not crime_id:
            return False, 'missing_crime_id'
        
        try:
            # Check if crime exists
            self.db_cursor.execute(f"SELECT 1 FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
            if not self.db_cursor.fetchone():
                self.stats['total_crimes_not_found'] += 1
                logger.debug(f"⚠️  Crime {crime_id} not found in database, skipping")
                return False, 'not_found'
            
            # Check current fir_copy value
            self.db_cursor.execute(f"SELECT fir_copy FROM {CRIMES_TABLE} WHERE crime_id = %s", (crime_id,))
            current_fir_copy = self.db_cursor.fetchone()[0]
            
            # Only update if value is different
            if current_fir_copy != fir_copy:
                update_query = f"""
                    UPDATE {CRIMES_TABLE}
                    SET fir_copy = %s
                    WHERE crime_id = %s
                """
                self.db_cursor.execute(update_query, (fir_copy, crime_id))
                self.db_conn.commit()
                self.stats['total_crimes_updated'] += 1
                logger.debug(f"Updated fir_copy for crime: {crime_id}")
                return True, 'updated'
            else:
                self.stats['total_crimes_no_change'] += 1
                # Use debug level here; trace level is not configured for this script
                logger.debug(f"No change needed for crime: {crime_id} (fir_copy already matches)")
                return True, 'no_change'
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error updating fir_copy for crime {crime_id}: {e}")
            self.stats['total_crimes_failed'] += 1
            self.stats['errors'].append(f"Crime {crime_id}: {str(e)}")
            return False, 'error'
    
    def process_date_range(self, from_date: str, to_date: str):
        """Process crimes for a specific date range"""
        chunk_range = f"{from_date} to {to_date}"
        logger.info(f"📅 Processing: {chunk_range}")
        
        # Fetch crimes from API
        crimes_raw = self.fetch_crimes_api(from_date, to_date)
        
        if crimes_raw is None:
            logger.error(f"❌ Failed to fetch crimes for {chunk_range}")
            return
        
        if not crimes_raw:
            logger.info(f"ℹ️  No crimes found for {chunk_range}")
            return
        
        # Process each crime
        self.stats['total_crimes_fetched'] += len(crimes_raw)
        
        for crime_raw in crimes_raw:
            crime_id = crime_raw.get('CRIME_ID')
            fir_copy = crime_raw.get('FIR_COPY')  # Can be None if not in API
            
            if not crime_id:
                logger.warning(f"⚠️  Crime missing CRIME_ID, skipping")
                self.stats['total_crimes_failed'] += 1
                continue
            
            success, operation = self.update_fir_copy(crime_id, fir_copy)
            
            if not success and operation == 'error':
                logger.warning(f"⚠️  Failed to update fir_copy for crime {crime_id}")
        
        logger.info(f"✅ Completed: {chunk_range} - Updated: {self.stats['total_crimes_updated']}, No Change: {self.stats['total_crimes_no_change']}, Not Found: {self.stats['total_crimes_not_found']}, Failed: {self.stats['total_crimes_failed']}")
    
    def run(self):
        """Main migration execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS FIR_COPY Migration Script")
        logger.info("=" * 80)
        logger.info("This script will backfill fir_copy column for existing crimes")
        logger.info("Only fir_copy column will be updated, other fields remain unchanged")
        logger.info("=" * 80)
        
        # Connect to database
        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False
        
        try:
            # Check if fir_copy column exists
            if not self.check_fir_copy_column():
                logger.error("Please add fir_copy column first using:")
                logger.error("  ALTER TABLE crimes ADD COLUMN fir_copy VARCHAR(50);")
                return False
            
            # For migration, always start from 2022-01-01 (full history),
            # similar chunking and overlap behaviour as etl_crimes.py
            fixed_start_date = '2022-01-01T00:00:00+05:30'
            calculated_end_date = get_yesterday_end_ist()
            
            logger.info(f"Fixed Start Date: {fixed_start_date}")
            logger.info(f"Calculated End Date: {calculated_end_date}")
            
            # Generate date ranges
            date_ranges = self.generate_date_ranges(
                fixed_start_date,
                calculated_end_date,
                ETL_CONFIG['chunk_days'],
                ETL_CONFIG.get('chunk_overlap_days', 1)
            )
            
            logger.info(f"Date Range: {fixed_start_date} to {calculated_end_date}")
            logger.info(f"Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {ETL_CONFIG.get('chunk_overlap_days', 1)} day(s))")
            logger.info("=" * 80)
            logger.info(f"📊 Total date ranges to process: {len(date_ranges)}")
            logger.info("")
            
            # Process each date range
            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                self.process_date_range(from_date, to_date)
                time.sleep(1)  # Be nice to the API
            
            # Print final statistics
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:      {self.stats['total_api_calls']}")
            logger.info(f"  Failed API Calls:     {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM API:")
            logger.info(f"  Total Crimes Fetched: {self.stats['total_crimes_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Updated:        {self.stats['total_crimes_updated']}")
            logger.info(f"  Total No Change:      {self.stats['total_crimes_no_change']}")
            logger.info(f"  Total Not Found:      {self.stats['total_crimes_not_found']}")
            logger.info(f"  Total Failed:         {self.stats['total_crimes_failed']}")
            logger.info(f"")
            logger.info(f"📊 SUMMARY:")
            logger.info(f"  Total from API:       {self.stats['total_crimes_fetched']}")
            logger.info(f"  Updated:              {self.stats['total_crimes_updated']}")
            logger.info(f"  No Change:            {self.stats['total_crimes_no_change']}")
            logger.info(f"  Not Found in DB:      {self.stats['total_crimes_not_found']}")
            logger.info(f"  Failed:               {self.stats['total_crimes_failed']}")
            logger.info("=" * 80)
            
            if self.stats['errors']:
                logger.warning("⚠️  Errors encountered:")
                for error in self.stats['errors'][:10]:
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more")
            
            logger.info("✅ Migration completed successfully!")
            return True
            
        except KeyboardInterrupt:
            logger.warning("\n⚠️  Migration interrupted by user")
            return False
        except Exception as e:
            logger.error(f"❌ Migration failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close_db()


def main():
    """Main entry point"""
    migration = FIRCopyMigration()
    success = migration.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()



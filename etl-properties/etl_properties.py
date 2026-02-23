#!/usr/bin/env python3
"""
DOPAMAS ETL Pipeline - Property Details API
Fetches property/seizure data in date-range chunks with overlap and loads into PostgreSQL
"""

import sys
import os
import time
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import colorlog
from typing import List, Dict, Optional, Tuple, Set
from datetime import timezone, timedelta

from config import DB_CONFIG, API_CONFIG, ETL_CONFIG, LOG_CONFIG, TABLE_CONFIG

# IST timezone offset (UTC+05:30)
IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

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
logger.setLevel(LOG_CONFIG['level'])

# Target table (allows redirecting ETL runs to test tables)
PROPERTIES_TABLE = TABLE_CONFIG.get('properties', 'properties')


def parse_iso_date(date_str: str) -> datetime:
    """Parse ISO 8601 date string (with optional time component) to datetime."""
    if 'T' in date_str:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return datetime.strptime(date_str, '%Y-%m-%d')


def get_yesterday_end_ist() -> str:
    """Get yesterday's date at 23:59:59 in IST (UTC+05:30) as ISO format string."""
    now_ist = datetime.now(IST_OFFSET)
    yesterday = now_ist - timedelta(days=1)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)
    return yesterday_end.isoformat()


class PropertiesETL:
    """ETL Pipeline for Property Details API"""
    
    def __init__(self):
        self.db_conn = None
        self.db_cursor = None
        self.stats = {
            'total_api_calls': 0,
            'total_properties_fetched': 0,
            'total_properties_inserted': 0,
            'total_properties_updated': 0,
            'total_properties_no_change': 0,  # Records that exist but no changes needed
            'total_properties_failed': 0,  # Records that failed to process
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
    
    def get_table_columns(self, table_name: str) -> Set[str]:
        """Get all column names from a table."""
        try:
            self.db_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table_name,))
            return {row[0] for row in self.db_cursor.fetchall()}
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {e}")
            return set()
    
    def get_effective_start_date(self) -> str:
        """
        Get effective start date for ETL:
        - If table is empty: return 2022-01-01T00:00:00+05:30
        - If table has data: return max(date_created, date_modified) from table
        """
        try:
            # Check if table has any data
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {PROPERTIES_TABLE}")
            count = self.db_cursor.fetchone()[0]
            
            if count == 0:
                # New database, start from beginning
                logger.info("📊 Table is empty, starting from 2022-01-01")
                return '2022-01-01T00:00:00+05:30'
            
            # Table has data, get max of date_created and date_modified
            # Only consider dates >= 2022-01-01 to avoid processing very old data
            MIN_START_DATE = '2022-01-01T00:00:00+05:30'
            min_start_dt = parse_iso_date('2022-01-01T00:00:00+05:30')
            
            self.db_cursor.execute(f"""
                SELECT GREATEST(
                    COALESCE(MAX(CASE WHEN date_created >= '2022-01-01'::timestamp THEN date_created END), '2022-01-01'::timestamp),
                    COALESCE(MAX(CASE WHEN date_modified >= '2022-01-01'::timestamp THEN date_modified END), '2022-01-01'::timestamp)
                ) as max_date
                FROM {PROPERTIES_TABLE}
            """)
            result = self.db_cursor.fetchone()
            if result and result[0]:
                max_date = result[0]
                # Convert to IST timezone if needed
                if isinstance(max_date, datetime):
                    if max_date.tzinfo is None:
                        max_date = max_date.replace(tzinfo=IST_OFFSET)
                    else:
                        max_date = max_date.astimezone(IST_OFFSET)
                    
                    # Ensure we never go before 2022-01-01
                    if max_date < min_start_dt:
                        logger.warning(f"⚠️  Max date ({max_date.isoformat()}) is before 2022-01-01, using 2022-01-01")
                        return MIN_START_DATE
                    
                    logger.info(f"📊 Table has data, starting from: {max_date.isoformat()}")
                    return max_date.isoformat()
            
            # Fallback to start date
            logger.warning("⚠️  Could not determine max date, using 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
            
        except Exception as e:
            logger.error(f"❌ Error getting effective start date: {e}")
            logger.warning("⚠️  Using default start date: 2022-01-01")
            return '2022-01-01T00:00:00+05:30'
    
    def detect_new_fields(self, api_record: Dict, table_columns: Set[str]) -> Dict[str, str]:
        """
        Detect new fields in API response that don't exist in table.
        Returns dict mapping API field name to database column name (snake_case).
        """
        new_fields = {}
        
        # Map API field names to database column names
        field_mapping = {
            'PROPERTY_ID': 'property_id',
            'CRIME_ID': 'crime_id',
            'CASE_PROPERTY_ID': 'case_property_id',
            'PROPERTY_STATUS': 'property_status',
            'RECOVERED_FROM': 'recovered_from',
            'PLACE_OF_RECOVERY': 'place_of_recovery',
            'DATE_OF_SEIZURE': 'date_of_seizure',
            'NATURE': 'nature',
            'BELONGS': 'belongs',
            'ESTIMATE_VALUE': 'estimate_value',
            'RECOVERED_VALUE': 'recovered_value',
            'PARTICULAR_OF_PROPERTY': 'particular_of_property',
            'CATEGORY': 'category',
            'ADDITIONAL_DETAILS': 'additional_details',
            'MEDIA': 'media',
            'DATE_CREATED': 'date_created',
            'DATE_MODIFIED': 'date_modified'
        }
        
        for api_field, db_column in field_mapping.items():
            if api_field in api_record and db_column not in table_columns:
                new_fields[api_field] = db_column
        
        return new_fields
    
    def add_column_to_table(self, column_name: str, column_type: str = 'TEXT'):
        """Add a new column to the properties table."""
        try:
            # Determine column type based on field name
            if 'date' in column_name.lower():
                column_type = 'TIMESTAMP'
            elif 'id' in column_name.lower() or 'code' in column_name.lower():
                column_type = 'VARCHAR(50)'
            elif column_name in ('additional_details', 'media'):
                column_type = 'JSONB'
            elif column_name in ('particular_of_property', 'nature'):
                column_type = 'TEXT'
            elif 'value' in column_name.lower():
                column_type = 'NUMERIC'
            else:
                column_type = 'VARCHAR(255)'
            
            alter_sql = f"ALTER TABLE {PROPERTIES_TABLE} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            self.db_cursor.execute(alter_sql)
            self.db_conn.commit()
            logger.info(f"✅ Added column {column_name} ({column_type}) to {PROPERTIES_TABLE}")
            return True
        except Exception as e:
            logger.error(f"❌ Error adding column {column_name}: {e}")
            self.db_conn.rollback()
            return False
    
    def update_existing_records_with_new_fields(self, new_fields: Dict[str, str], chunk_end_date: str):
        """
        Update existing records from start_date to chunk_end_date with new fields.
        For new fields, set to NULL (they will be updated when those records are processed).
        """
        if not new_fields:
            return
        
        try:
            logger.info(f"📝 New fields detected: {list(new_fields.keys())}")
            logger.info(f"   Note: Existing records will be updated when processed in future ETL runs")
            logger.info(f"   New fields are set to NULL for existing records until they are reprocessed")
        except Exception as e:
            logger.error(f"❌ Error updating existing records: {e}")
    
    def generate_date_ranges(self, start_date: str, end_date: str, chunk_days: int = 5, overlap_days: int = 1) -> List[Tuple[str, str]]:
        """
        Generate date ranges in chunks with overlap to ensure no data is missed
        OVERLAP: Each chunk overlaps with the previous chunk by overlap_days to catch boundary records
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_days: Number of days per chunk
            overlap_days: Number of days to overlap between chunks (default: 1 to ensure no data loss)
        
        Returns:
            List of (from_date, to_date) tuples in YYYY-MM-DD format
        """
        date_ranges = []
        current_date = parse_iso_date(start_date).date()
        end = parse_iso_date(end_date).date()
        
        while current_date <= end:
            chunk_end = current_date + timedelta(days=chunk_days - 1)
            if chunk_end > end:
                chunk_end = end
            
            # Always add the range, even if it's less than chunk_days (handles partial chunks)
            date_ranges.append((
                current_date.strftime('%Y-%m-%d'),
                chunk_end.strftime('%Y-%m-%d')
            ))
            
            # Next chunk starts with overlap: current chunk end - overlap_days + 1
            # This ensures the last overlap_days of current chunk are included in next chunk
            # Example: If chunk_end = 2022-10-05 and overlap_days = 1:
            #   next chunk starts at 2022-10-05 - 1 + 1 = 2022-10-05 (includes day 5 in both chunks)
            next_start = chunk_end - timedelta(days=overlap_days - 1)
            
            # If we've already reached or passed the end date, break
            if chunk_end >= end:
                break
            
            # Move to next chunk start
            current_date = next_start
        
        return date_ranges
    
    def fetch_properties_api(self, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """
        Fetch properties from API for given date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            List of property dicts or None if failed
        """
        url = f"{API_CONFIG['base_url']}/property-details"
        params = {
            'fromDate': from_date,
            'toDate': to_date
        }
        headers = {
            'x-api-key': API_CONFIG['api_key']
        }
        
        for attempt in range(API_CONFIG['max_retries']):
            try:
                logger.debug(f"Fetching properties: {from_date} to {to_date} (Attempt {attempt + 1})")
                response = requests.get(
                    url,
                    params=params,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.stats['total_api_calls'] += 1
                    
                    if data.get('status'):
                        property_data = data.get('data')
                        if property_data:
                            # Ensure it's a list
                            if isinstance(property_data, dict):
                                property_data = [property_data]
                            logger.info(f"✅ Fetched {len(property_data)} properties for {from_date} to {to_date}")
                            return property_data
                        else:
                            logger.warning(f"⚠️  No properties found for {from_date} to {to_date}")
                            return []
                    else:
                        logger.warning(f"⚠️  API returned status=false for {from_date} to {to_date}")
                        return []
                
                elif response.status_code == 404:
                    logger.warning(f"⚠️  No data found for {from_date} to {to_date}")
                    return []
                
                else:
                    logger.warning(f"API returned status code {response.status_code}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
            except requests.exceptions.Timeout:
                logger.warning(f"API timeout, retrying... (Attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API error: {e}")
                if attempt == API_CONFIG['max_retries'] - 1:
                    self.stats['failed_api_calls'] += 1
                    self.stats['errors'].append(f"{from_date} to {to_date}: {str(e)}")
                time.sleep(2 ** attempt)
        
        logger.error(f"❌ Failed to fetch properties for {from_date} to {to_date} after {API_CONFIG['max_retries']} attempts")
        return None
    
    def parse_date_field(self, date_value) -> Optional[datetime]:
        """
        Parse date field from API response
        Handles ISO 8601 format, empty strings, None, and already-parsed datetime objects
        
        Args:
            date_value: Date value from API (string, datetime, or None)
        
        Returns:
            datetime object or None
        """
        if date_value is None:
            return None
        
        # If already a datetime object, return as is
        if isinstance(date_value, datetime):
            return date_value
        
        # If not a string, try to convert
        if not isinstance(date_value, str):
            return None
        
        # Handle empty strings
        if not date_value.strip():
            return None
        
        try:
            # Try parsing ISO format (handles 'Z' timezone)
            return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            # If ISO parsing fails, try other common formats
            try:
                # Try YYYY-MM-DD format
                return datetime.strptime(date_value, '%Y-%m-%d')
            except (ValueError, AttributeError):
                # If all parsing fails, return None
                logger.debug(f"Could not parse date: {date_value}")
                return None
    
    def transform_property(self, property_raw: Dict) -> Dict:
        """
        Transform API response to database format
        Dates are always taken from API (never use CURRENT_TIMESTAMP)
        
        Args:
            property_raw: Raw property data from API
        
        Returns:
            Transformed property dict ready for database
        """
        # Parse date_of_seizure (handle ISO format, empty strings, None)
        date_of_seizure = self.parse_date_field(property_raw.get('DATE_OF_SEIZURE'))
        
        # Parse date_created and date_modified (handle ISO format, empty strings, None)
        date_created = self.parse_date_field(property_raw.get('DATE_CREATED'))
        date_modified = self.parse_date_field(property_raw.get('DATE_MODIFIED'))
        
        # Handle ADDITIONAL_DETAILS - ensure it's a dict
        additional_details = property_raw.get('ADDITIONAL_DETAILS', {})
        if not isinstance(additional_details, dict):
            additional_details = {}
        
        # Handle MEDIA - ensure it's a list
        media = property_raw.get('MEDIA', [])
        if not isinstance(media, list):
            media = []
        
        return {
            'property_id': property_raw.get('PROPERTY_ID'),
            'crime_id': property_raw.get('CRIME_ID'),
            'case_property_id': property_raw.get('CASE_PROPERTY_ID'),
            'property_status': property_raw.get('PROPERTY_STATUS'),
            'recovered_from': property_raw.get('RECOVERED_FROM'),
            'place_of_recovery': property_raw.get('PLACE_OF_RECOVERY'),
            'date_of_seizure': date_of_seizure,
            'nature': property_raw.get('NATURE'),
            'belongs': property_raw.get('BELONGS'),
            'estimate_value': property_raw.get('ESTIMATE_VALUE', 0),
            'recovered_value': property_raw.get('RECOVERED_VALUE', 0),
            'particular_of_property': property_raw.get('PARTICULAR_OF_PROPERTY'),
            'category': property_raw.get('CATEGORY'),
            'additional_details': additional_details,
            'media': media,
            'date_created': date_created,  # Parsed from API (or NULL)
            'date_modified': date_modified  # Parsed from API (or NULL)
        }
    
    def property_exists(self, property_id: str) -> bool:
        """Check if property already exists in database"""
        self.db_cursor.execute(f"SELECT 1 FROM {PROPERTIES_TABLE} WHERE property_id = %s", (property_id,))
        return self.db_cursor.fetchone() is not None
    
    def insert_property(self, prop: Dict) -> bool:
        """
        Insert or update single property in database
        
        Args:
            prop: Transformed property dict
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if property already exists
            if self.property_exists(prop['property_id']):
                # Update existing property
                # Note: date_created is NOT updated (preserved from original insert)
                # date_modified comes from API (as per API response)
                update_query = f"""
                    UPDATE {PROPERTIES_TABLE} SET
                        crime_id = %s,
                        case_property_id = %s,
                        property_status = %s,
                        recovered_from = %s,
                        place_of_recovery = %s,
                        date_of_seizure = %s,
                        nature = %s,
                        belongs = %s,
                        estimate_value = %s,
                        recovered_value = %s,
                        particular_of_property = %s,
                        category = %s,
                        additional_details = %s,
                        media = %s,
                        date_modified = %s
                    WHERE property_id = %s
                """
                self.db_cursor.execute(update_query, (
                    prop['crime_id'],
                    prop['case_property_id'],
                    prop['property_status'],
                    prop['recovered_from'],
                    prop['place_of_recovery'],
                    prop['date_of_seizure'],
                    prop['nature'],
                    prop['belongs'],
                    prop['estimate_value'],
                    prop['recovered_value'],
                    prop['particular_of_property'],
                    prop['category'],
                    Json(prop['additional_details']),
                    Json(prop['media']),
                    prop['date_modified'],  # Only update date_modified from API
                    prop['property_id']
                ))
                self.stats['total_properties_updated'] += 1
                logger.debug(f"Updated property: {prop['property_id']}")
            else:
                # Insert new property
                insert_query = f"""
                    INSERT INTO {PROPERTIES_TABLE} (
                        property_id, crime_id, case_property_id, property_status,
                        recovered_from, place_of_recovery, date_of_seizure, nature,
                        belongs, estimate_value, recovered_value, particular_of_property,
                        category, additional_details, media,
                        date_created, date_modified
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                self.db_cursor.execute(insert_query, (
                    prop['property_id'],
                    prop['crime_id'],
                    prop['case_property_id'],
                    prop['property_status'],
                    prop['recovered_from'],
                    prop['place_of_recovery'],
                    prop['date_of_seizure'],
                    prop['nature'],
                    prop['belongs'],
                    prop['estimate_value'],
                    prop['recovered_value'],
                    prop['particular_of_property'],
                    prop['category'],
                    Json(prop['additional_details']),
                    Json(prop['media']),
                    prop['date_created'],
                    prop['date_modified']
                ))
                self.stats['total_properties_inserted'] += 1
                logger.debug(f"Inserted property: {prop['property_id']}")
            
            self.db_conn.commit()
            return True
            
        except psycopg2.IntegrityError as e:
            self.db_conn.rollback()
            logger.warning(f"⚠️  Integrity error for property {prop['property_id']}: {e}")
            self.stats['total_properties_failed'] += 1
            return False
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Error inserting property {prop['property_id']}: {e}")
            self.stats['errors'].append(f"Property {prop['property_id']}: {str(e)}")
            return False
    
    def process_date_range(self, from_date: str, to_date: str, table_columns: Set[str] = None):
        """Process properties for a specific date range"""
        logger.info(f"📅 Processing: {from_date} to {to_date}")
        
        # Initialize chunk-level statistics
        chunk_stats = {
            'inserted': 0,
            'updated': 0,
            'no_change': 0,
            'failed': 0
        }
        
        # Store initial stats to calculate chunk differences
        initial_inserted = self.stats['total_properties_inserted']
        initial_updated = self.stats['total_properties_updated']
        initial_no_change = self.stats['total_properties_no_change']
        initial_failed = self.stats['total_properties_failed']
        
        # Fetch properties from API
        properties_raw = self.fetch_properties_api(from_date, to_date)
        
        if properties_raw is None:
            logger.error(f"❌ Failed to fetch properties for {from_date} to {to_date}")
            chunk_stats['failed'] = 1  # API call failed
            self.stats['total_properties_failed'] += 1
            return
        
        if not properties_raw:
            logger.info(f"ℹ️  No properties found for {from_date} to {to_date} - continuing to next chunk")
            return
        
        # Check for schema evolution if we got data
        if table_columns is not None and len(properties_raw) > 0:
            # Check for new fields in first record
            new_fields = self.detect_new_fields(properties_raw[0], table_columns)
            if new_fields:
                logger.info(f"🔍 New fields detected in API response: {list(new_fields.keys())}")
                # Add new columns to table
                for api_field, db_column in new_fields.items():
                    if self.add_column_to_table(db_column):
                        # Update table_columns set
                        table_columns.add(db_column)
                # Update existing records from start_date to current chunk end_date
                self.update_existing_records_with_new_fields(new_fields, to_date)
        
        # Transform and insert each property
        self.stats['total_properties_fetched'] += len(properties_raw)
        
        for property_raw in properties_raw:
            prop = self.transform_property(property_raw)
            if prop['property_id']:
                result = self.insert_property(prop)
                if not result:
                    chunk_stats['failed'] += 1
            else:
                logger.warning(f"⚠️  Property missing PROPERTY_ID, skipping")
                chunk_stats['failed'] += 1
                self.stats['total_properties_failed'] += 1
        
        # Calculate chunk statistics
        chunk_stats['inserted'] = self.stats['total_properties_inserted'] - initial_inserted
        chunk_stats['updated'] = self.stats['total_properties_updated'] - initial_updated
        chunk_stats['no_change'] = self.stats['total_properties_no_change'] - initial_no_change
        chunk_stats['failed'] = self.stats['total_properties_failed'] - initial_failed
        
        # Log chunk statistics
        logger.info(f"✅ Completed: {from_date} to {to_date}")
        logger.info(f"   📊 Chunk Stats - Inserted: {chunk_stats['inserted']}, Updated: {chunk_stats['updated']}, "
                   f"No Change: {chunk_stats['no_change']}, Failed: {chunk_stats['failed']}")
    
    def run(self):
        """Main ETL execution"""
        logger.info("=" * 80)
        logger.info("🚀 DOPAMAS ETL Pipeline - Property Details API")
        logger.info("=" * 80)
        
        # Calculate date range
        # Start date: Always 2022-01-01T00:00:00+05:30
        # End date: Yesterday at 23:59:59+05:30 (IST)
        fixed_start_date = '2022-01-01T00:00:00+05:30'
        calculated_end_date = get_yesterday_end_ist()
        
        logger.info(f"Fixed Start Date: {fixed_start_date}")
        logger.info(f"Calculated End Date: {calculated_end_date}")
        
        # Connect to database
        if not self.connect_db():
            logger.error("Failed to connect to database. Exiting.")
            return False
        
        try:
            # Get effective start date (check if table has data)
            effective_start_date = self.get_effective_start_date()
            logger.info(f"Effective Start Date: {effective_start_date}")
            
            # Get table columns for schema evolution
            table_columns = self.get_table_columns(PROPERTIES_TABLE)
            logger.debug(f"Existing table columns: {sorted(table_columns)}")
            
            # Generate date ranges with overlap to ensure no data is missed
            date_ranges = self.generate_date_ranges(
                effective_start_date,
                calculated_end_date,
                ETL_CONFIG['chunk_days'],
                ETL_CONFIG.get('chunk_overlap_days', 1)  # Default to 1 day overlap for safety
            )
            
            logger.info(f"Date Range: {effective_start_date} to {calculated_end_date}")
            overlap_days = ETL_CONFIG.get('chunk_overlap_days', 1)
            logger.info(f"Chunk Size: {ETL_CONFIG['chunk_days']} days (overlap: {overlap_days} day(s) to ensure no data loss)")
            logger.info("=" * 80)
            
            logger.info(f"📊 Total date ranges to process: {len(date_ranges)}")
            logger.info("")
            
            # Process each date range with progress bar
            for from_date, to_date in tqdm(date_ranges, desc="Processing date ranges", unit="range"):
                # Process the chunk (will check for schema evolution and process data)
                self.process_date_range(from_date, to_date, table_columns)
                time.sleep(1)  # Be nice to the API
            
            # Get database counts
            self.db_cursor.execute(f"SELECT COUNT(*) FROM {PROPERTIES_TABLE}")
            db_properties_count = self.db_cursor.fetchone()[0]
            
            # Print final statistics
            logger.info("")
            logger.info("=" * 80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("=" * 80)
            logger.info(f"📡 API CALLS:")
            logger.info(f"  Total API Calls:          {self.stats['total_api_calls']}")
            logger.info(f"  Failed API Calls:         {self.stats['failed_api_calls']}")
            logger.info(f"")
            logger.info(f"📥 FROM API:")
            logger.info(f"  Total Properties Fetched: {self.stats['total_properties_fetched']}")
            logger.info(f"")
            logger.info(f"💾 TO DATABASE:")
            logger.info(f"  Total Inserted (New):     {self.stats['total_properties_inserted']}")
            logger.info(f"  Total Updated:            {self.stats['total_properties_updated']}")
            logger.info(f"  Total No Change:          {self.stats['total_properties_no_change']}")
            logger.info(f"  Total Failed:             {self.stats['total_properties_failed']}")
            logger.info(f"  Total in DB:              {db_properties_count}")
            logger.info(f"")
            logger.info(f"📊 COVERAGE:")
            if self.stats['total_properties_fetched'] > 0:
                coverage = ((self.stats['total_properties_inserted'] + self.stats['total_properties_updated']) / self.stats['total_properties_fetched']) * 100
                logger.info(f"  API → DB Coverage:       {coverage:.2f}%")
            logger.info(f"")
            logger.info(f"❌ Errors:                   {len(self.stats['errors'])}")
            logger.info("=" * 80)
            
            if self.stats['errors']:
                logger.warning("⚠️  Errors encountered:")
                for error in self.stats['errors'][:10]:  # Show first 10 errors
                    logger.warning(f"  - {error}")
                if len(self.stats['errors']) > 10:
                    logger.warning(f"  ... and {len(self.stats['errors']) - 10} more")
            
            logger.info("✅ ETL Pipeline completed successfully!")
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
            self.close_db()


def main():
    """Main entry point"""
    etl = PropertiesETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()


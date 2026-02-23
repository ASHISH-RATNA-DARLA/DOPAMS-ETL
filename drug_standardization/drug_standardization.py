"""
Drug Name Standardization Script
Automatically updates primary_drug_name based on drug_name mappings
Author: Auto-generated
Date: 2026-01-28
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import psycopg2
from psycopg2 import Error
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Configure logging - console only, no file output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DrugStandardizer:
    """Main class to handle drug name standardization"""
    
    def __init__(self):
        """Initialize database connection and load mappings"""
        self.connection = None
        self.cursor = None
        self.table_name = os.getenv('TABLE_NAME')
        if not self.table_name:
            logger.error("TABLE_NAME not found in .env file!")
            sys.exit(1)
        self.drug_mappings = self._load_drug_mappings()
        self.stats = {
            'total_processed': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'unmatched': []
        }
    
    def _normalize_drug_name(self, drug_name: str) -> str:
        """
        Normalize drug name by converting to lowercase and removing spaces
        
        Args:
            drug_name: Original drug name
            
        Returns:
            Normalized drug name
        """
        if not drug_name:
            return ""
        return drug_name.lower().replace(" ", "")
    
    def _load_drug_mappings(self) -> Dict[str, str]:
        """
        Load drug mappings from the mappings file
        
        Returns:
            Dictionary mapping normalized drug names to primary drug names
        """
        try:
            with open('drug_mappings.json', 'r', encoding='utf-8') as f:
                mappings = json.load(f)
            logger.info(f"Loaded {len(mappings)} drug mappings")
            return mappings
        except FileNotFoundError:
            logger.error("drug_mappings.json not found! Please ensure the file exists.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing drug_mappings.json: {e}")
            sys.exit(1)
    
    def connect_to_database(self) -> bool:
        """
        Establish connection to PostgreSQL database
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            db_info = self.connection.get_dsn_parameters()
            logger.info(f"Successfully connected to PostgreSQL")
            logger.info(f"Connected to database: {os.getenv('DB_NAME')}")
            logger.info(f"Using table: {self.table_name}")
            return True
                
        except Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            return False
    
    def fetch_records_to_update(self) -> List[Dict]:
        """
        Fetch records that need to be updated
        
        Returns:
            List of records with id and drug_name
        """
        try:
            # Fetch records where primary_drug_name is NULL or empty
            # Use parameterized query with table name (quoted for PostgreSQL)
            # Also handles string 'NULL' (case-insensitive) which is a common mistake
            query = f"""
                SELECT id, drug_name, primary_drug_name 
                FROM "{self.table_name}"
                WHERE primary_drug_name IS NULL 
                   OR primary_drug_name = '' 
                   OR primary_drug_name = 'Unknown'
                   OR (primary_drug_name IS NOT NULL AND LOWER(TRIM(primary_drug_name)) = 'null')
            """
            
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            logger.info(f"Found {len(records)} records to process")
            if len(records) > 0:
                # Log a sample of what we found for debugging
                sample = records[:3] if len(records) >= 3 else records
                logger.info(f"Sample records found: {[(r['id'], r.get('drug_name'), r.get('primary_drug_name')) for r in sample]}")
            return records
            
        except Error as e:
            logger.error(f"Error fetching records: {e}")
            return []
    
    def update_record(self, record_id: int, primary_drug_name: str) -> bool:
        """
        Update a single record with the primary drug name
        
        Args:
            record_id: ID of the record to update
            primary_drug_name: Value to set for primary_drug_name
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            # Use parameterized query with table name (quoted for PostgreSQL)
            query = f"""
                UPDATE "{self.table_name}"
                SET primary_drug_name = %s 
                WHERE id = %s
            """
            
            self.cursor.execute(query, (primary_drug_name, record_id))
            return True
            
        except Error as e:
            logger.error(f"Error updating record {record_id}: {e}")
            return False
    
    def process_records(self) -> None:
        """
        Main processing logic - fetch and update records
        """
        records = self.fetch_records_to_update()
        
        if not records:
            logger.info("No records to process")
            return
        
        logger.info("Starting record processing...")
        
        for record in records:
            self.stats['total_processed'] += 1
            record_id = record['id']
            drug_name = record['drug_name']
            
            if not drug_name or drug_name.strip() == '':
                logger.warning(f"Record {record_id}: Empty drug_name, skipping")
                self.stats['skipped'] += 1
                continue
            
            # Normalize the drug name
            normalized_name = self._normalize_drug_name(drug_name)
            
            # Look up in mappings
            if normalized_name in self.drug_mappings:
                primary_drug_name = self.drug_mappings[normalized_name]
                
                # Update the record
                if self.update_record(record_id, primary_drug_name):
                    self.stats['updated'] += 1
                    logger.info(f"Record {record_id}: '{drug_name}' -> '{primary_drug_name}'")
                else:
                    self.stats['errors'] += 1
            else:
                # No mapping found - set primary_drug_name = drug_name (original value)
                if self.update_record(record_id, drug_name):
                    self.stats['updated'] += 1
                    logger.info(f"Record {record_id}: No mapping found, set primary_drug_name = '{drug_name}' (original value)")
                else:
                    self.stats['errors'] += 1
                
                # Still track as unmatched for reporting purposes
                self.stats['unmatched'].append({
                    'id': record_id,
                    'drug_name': drug_name,
                    'normalized': normalized_name
                })
        
        # Commit all changes
        try:
            self.connection.commit()
            logger.info("All changes committed successfully")
        except Error as e:
            logger.error(f"Error committing changes: {e}")
            self.connection.rollback()
            logger.info("Changes rolled back")
    
    def generate_report(self) -> None:
        """Generate and save a summary report"""
        report = f"""
{'='*80}
DRUG STANDARDIZATION REPORT
{'='*80}
Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY:
--------
Total Records Processed: {self.stats['total_processed']}
Successfully Updated:     {self.stats['updated']}
Skipped (Empty):          {self.stats['skipped']}
Errors:                   {self.stats['errors']}
Unmatched:                {len(self.stats['unmatched'])}

"""
        
        if self.stats['unmatched']:
            report += f"\nUNMATCHED DRUG NAMES ({len(self.stats['unmatched'])} records):\n"
            report += "-" * 80 + "\n"
            for item in self.stats['unmatched'][:50]:  # Show first 50
                # Handle both UUID (string) and integer IDs
                id_str = str(item['id'])
                report += f"ID: {id_str} | Drug Name: {item['drug_name']}\n"
            
            if len(self.stats['unmatched']) > 50:
                report += f"\n... and {len(self.stats['unmatched']) - 50} more unmatched records\n"
        
        report += "=" * 80 + "\n"
        
        # Print to console only (no file output)
        print(report)
    
    def close_connection(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def run(self) -> None:
        """Main execution method"""
        try:
            logger.info("=" * 80)
            logger.info("DRUG STANDARDIZATION SCRIPT STARTED")
            logger.info("=" * 80)
            
            # Connect to database
            if not self.connect_to_database():
                logger.error("Failed to connect to database. Exiting.")
                sys.exit(1)
            
            # Process records
            self.process_records()
            
            # Generate report
            self.generate_report()
            
            logger.info("=" * 80)
            logger.info("DRUG STANDARDIZATION SCRIPT COMPLETED")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            if self.connection:
                self.connection.rollback()
                logger.info("Transaction rolled back due to error")
        
        finally:
            self.close_connection()


def main():
    """Entry point of the script"""
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("ERROR: .env file not found!")
        print("Please create a .env file with database credentials")
        print("\nExample .env file:")
        print("-" * 50)
        print("DB_HOST=192.168.103.106")
        print("DB_PORT=5432")
        print("DB_NAME=your_database_name")
        print("DB_USER=your_username")
        print("DB_PASSWORD=your_password")
        print("TABLE_NAME=your_table_name")
        print("-" * 50)
        sys.exit(1)
    
    # Check if mappings file exists
    if not os.path.exists('drug_mappings.json'):
        print("ERROR: drug_mappings.json file not found!")
        print("Please ensure the drug_mappings.json file is in the same directory")
        sys.exit(1)
    
    # Run the standardizer
    standardizer = DrugStandardizer()
    standardizer.run()


if __name__ == "__main__":
    main()


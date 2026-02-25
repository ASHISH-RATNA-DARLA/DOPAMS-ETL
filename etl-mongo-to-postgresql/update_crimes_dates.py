"""
Update script to fix crimes table date mappings
Updates only fir_date and date_created columns based on correct MongoDB field mappings:
- FROM_DT -> date_created
- REG_DT -> fir_date
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional

import pymongo
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'update_crimes_dates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class CrimesDateUpdater:
    """Update crimes table date fields"""
    
    def __init__(self):
        """Initialize connections"""
        self.mongo_client = None
        self.mongo_db = None
        self.pg_pool = None
        self.stats = {
            'total_crimes': 0,
            'updated': 0,
            'not_found_in_mongo': 0,
            'errors': 0
        }
        
    def connect_mongodb(self):
        """Connect to MongoDB"""
        try:
            mongo_uri = os.getenv('MONGO_URI')
            mongo_db_name = os.getenv('MONGO_DB_NAME')
            
            if not mongo_uri or not mongo_db_name:
                raise ValueError("MONGO_URI and MONGO_DB_NAME must be set in .env")
                
            self.mongo_client = MongoClient(mongo_uri)
            self.mongo_db = self.mongo_client[mongo_db_name]
            
            # Test connection
            self.mongo_client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {mongo_db_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
            
    def connect_postgresql(self):
        """Connect to PostgreSQL"""
        try:
            pg_config = {
                'host': os.getenv('POSTGRES_HOST'),
                'port': os.getenv('POSTGRES_PORT'),
                'database': os.getenv('POSTGRES_DB'),
                'user': os.getenv('POSTGRES_USER'),
                'password': os.getenv('POSTGRES_PASSWORD')
            }
            
            if not all([pg_config['host'], pg_config['database'], pg_config['user'], pg_config['password']]):
                raise ValueError("PostgreSQL connection details must be set in .env")
                
            self.pg_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **pg_config
            )
            
            # Test connection
            conn = self.pg_pool.getconn()
            conn.close()
            self.pg_pool.putconn(conn)
            
            logger.info(f"Connected to PostgreSQL: {pg_config['database']}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
            
    def get_pg_connection(self):
        """Get PostgreSQL connection from pool"""
        return self.pg_pool.getconn()
        
    def return_pg_connection(self, conn):
        """Return connection to pool"""
        self.pg_pool.putconn(conn)
        
    def parse_date(self, date_value) -> Optional[datetime]:
        """Parse date from various formats"""
        if not date_value:
            return None
            
        try:
            if isinstance(date_value, datetime):
                return date_value
            elif isinstance(date_value, str):
                # Try ISO format
                return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            else:
                return None
        except Exception as e:
            logger.debug(f"Error parsing date {date_value}: {e}")
            return None
            
    def update_crime_dates(self, crime_id: str, from_dt: Optional[datetime], 
                          reg_dt: Optional[datetime], conn) -> bool:
        """Update fir_date and date_created for a crime record"""
        cursor = conn.cursor()
        
        try:
            # FROM_DT should go to date_created
            # REG_DT should go to fir_date
            cursor.execute("""
                UPDATE crimes
                SET 
                    date_created = %s,
                    fir_date = %s,
                    date_modified = %s
                WHERE crime_id = %s
            """, (
                from_dt,  # FROM_DT -> date_created
                reg_dt,   # REG_DT -> fir_date
                datetime.now(),  # Update date_modified
                crime_id
            ))
            
            if cursor.rowcount > 0:
                conn.commit()
                logger.info(f"Updated crime {crime_id}: date_created={from_dt}, fir_date={reg_dt}")
                return True
            else:
                logger.warning(f"No rows updated for crime_id: {crime_id}")
                return False
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating crime {crime_id}: {e}")
            return False
        finally:
            cursor.close()
            
    def run(self):
        """Run the update process"""
        logger.info("=" * 80)
        logger.info("Starting Crimes Date Update Process")
        logger.info("=" * 80)
        logger.info("Mapping: FROM_DT -> date_created, REG_DT -> fir_date")
        logger.info("=" * 80)
        
        # Connect to databases
        if not self.connect_mongodb():
            logger.error("Failed to connect to MongoDB, aborting")
            return False
            
        if not self.connect_postgresql():
            logger.error("Failed to connect to PostgreSQL, aborting")
            return False
            
        try:
            # Get collection name
            collection_name = os.getenv('MONGO_COLLECTION_NAME')
            collection = self.mongo_db[collection_name]
            
            # Get PostgreSQL connection
            conn = self.get_pg_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            try:
                # Get all crime records from PostgreSQL
                cursor.execute("SELECT crime_id FROM crimes ORDER BY crime_id")
                crimes = cursor.fetchall()
                self.stats['total_crimes'] = len(crimes)
                
                logger.info(f"Found {self.stats['total_crimes']} crime records to check")
                
                # Process each crime
                for idx, crime_row in enumerate(crimes, 1):
                    crime_id = crime_row['crime_id']
                    
                    if idx % 100 == 0:
                        logger.info(f"Progress: {idx}/{self.stats['total_crimes']} crimes processed")
                        self.print_stats()
                    
                    try:
                        # Find corresponding MongoDB record
                        # Try to find by _id (if crime_id is MongoDB ObjectId string)
                        mongo_record = None
                        
                        # Try direct ObjectId lookup
                        try:
                            from bson import ObjectId
                            mongo_record = collection.find_one({'_id': ObjectId(crime_id)})
                        except:
                            # If ObjectId conversion fails, try string match
                            mongo_record = collection.find_one({'_id': crime_id})
                        
                        if not mongo_record:
                            # Try as string
                            mongo_record = collection.find_one({'_id': str(crime_id)})
                        
                        if not mongo_record:
                            logger.warning(f"Crime {crime_id}: Not found in MongoDB")
                            self.stats['not_found_in_mongo'] += 1
                            continue
                        
                        # Get date values from MongoDB
                        from_dt_value = mongo_record.get('FROM_DT')
                        reg_dt_value = mongo_record.get('REG_DT')
                        
                        # Parse dates
                        from_dt = self.parse_date(from_dt_value)
                        reg_dt = self.parse_date(reg_dt_value)
                        
                        # Update the crime record
                        if self.update_crime_dates(crime_id, from_dt, reg_dt, conn):
                            self.stats['updated'] += 1
                        else:
                            self.stats['errors'] += 1
                            
                    except Exception as e:
                        logger.error(f"Error processing crime {crime_id}: {e}", exc_info=True)
                        self.stats['errors'] += 1
                        
            finally:
                cursor.close()
                self.return_pg_connection(conn)
            
            # Print final stats
            logger.info("=" * 80)
            logger.info("Crimes Date Update Completed")
            logger.info("=" * 80)
            self.print_stats()
            
            return True
            
        except Exception as e:
            logger.error(f"Error during update: {e}", exc_info=True)
            return False
        finally:
            # Close connections
            if self.mongo_client:
                self.mongo_client.close()
            if self.pg_pool:
                self.pg_pool.closeall()
                
    def print_stats(self):
        """Print update statistics"""
        logger.info("Update Statistics:")
        logger.info(f"  Total Crimes: {self.stats['total_crimes']}")
        logger.info(f"  Updated: {self.stats['updated']}")
        logger.info(f"  Not Found in MongoDB: {self.stats['not_found_in_mongo']}")
        logger.info(f"  Errors: {self.stats['errors']}")


if __name__ == '__main__':
    updater = CrimesDateUpdater()
    updater.run()



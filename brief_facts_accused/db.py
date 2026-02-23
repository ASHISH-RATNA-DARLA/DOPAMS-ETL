
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import config

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise

def fetch_crimes_by_ids(conn, crime_ids):
    """
    Fetches specific crimes based on a list of IDs.
    """
    if not crime_ids:
        return []
        
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = "SELECT crime_id, brief_facts FROM crimes WHERE crime_id = ANY(%s)"
        cur.execute(query, (crime_ids,))
        return cur.fetchall()

def fetch_unprocessed_crimes(conn, limit=100):
    """
    Fetches crimes that do NOT yet have an entry in the configured accused table.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = sql.SQL("""
            SELECT c.crime_id, c.brief_facts 
            FROM crimes c
            LEFT JOIN {table} d ON c.crime_id = d.crime_id
            WHERE d.crime_id IS NULL
            ORDER BY c.date_created DESC, c.date_modified DESC
            LIMIT %s
        """).format(table=sql.Identifier(config.ACCUSED_TABLE_NAME))
        
        cur.execute(query, (limit,))
        return cur.fetchall()

def fetch_existing_accused_for_crime(conn, crime_id):
    """
    Fetches known accused/persons linked to a specific crime.
    Used for matching extracted entities to database records.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Join accused and persons to get the full name for matching
        # Note: We prioritize the person's real full_name from persons table
        query = """
            SELECT 
                a.accused_id,
                a.person_id,
                p.full_name,
                p.alias,
                a.type as accused_type,
                p.age,
                p.gender,
                p.occupation,
                NULLIF(TRIM(CONCAT_WS(', ', 
                    p.present_house_no, 
                    p.present_street_road_no, 
                    p.present_locality_village, 
                    p.present_area_mandal, 
                    p.present_district, 
                    p.present_state_ut
                )), '') as address,
                p.phone_number as phone_numbers
            FROM accused a
            JOIN persons p ON a.person_id = p.person_id
            WHERE a.crime_id = %s
        """
        cur.execute(query, (crime_id,))
        return cur.fetchall()

def truncate_varchar(value, max_length=255):
    """Truncate string values to fit VARCHAR constraints."""
    if value is None:
        return None
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value

def validate_age(age_value):
    """
    Validate and sanitize age values for database insertion.
    Returns None if invalid, otherwise returns an integer between 0-150.
    """
    if age_value is None:
        return None
    
    # Convert string to int if possible
    try:
        if isinstance(age_value, str):
            # Extract first number from string (e.g., "25 years" -> 25)
            import re
            match = re.search(r'\d+', str(age_value))
            if match:
                age_value = int(match.group())
            else:
                return None
        
        age_int = int(age_value)
        
        # Validate reasonable age range (0-150)
        if age_int < 0 or age_int > 150:
            return None
        
        return age_int
    except (ValueError, TypeError, OverflowError):
        # If conversion fails or value is out of range, return None
        return None

def insert_accused_facts(conn, item_data):
    """Inserts extracted accused information into the database."""
    with conn.cursor() as cur:
        query = sql.SQL("""
            INSERT INTO {table} 
            (
                bf_accused_id, crime_id, 
                accused_id, person_id, existing_accused,
                full_name, alias_name, age, gender, occupation, address, phone_numbers,
                role_in_crime, key_details, accused_type, status, is_ccl,
                source_person_fields, source_accused_fields, source_summary_fields
            )
            VALUES (
                %s, %s, 
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (crime_id, accused_id) DO NOTHING
        """).format(table=sql.Identifier(config.ACCUSED_TABLE_NAME))
        
        import json
        import uuid
        
        # Generate ID if not present (though we usually generate it here)
        bf_id = str(uuid.uuid4())
        
        # Truncate VARCHAR fields to prevent constraint violations
        alias_name = truncate_varchar(item_data.get('alias_name'), 255)
        occupation = truncate_varchar(item_data.get('occupation'), 255)
        phone_numbers = truncate_varchar(item_data.get('phone_numbers'), 255)
        gender = truncate_varchar(item_data.get('gender'), 20)  # VARCHAR(20) constraint
        
        # Validate age field to prevent integer out of range errors
        age = validate_age(item_data.get('age'))
        
        cur.execute(query, (
            bf_id,
            item_data.get('crime_id'),
            item_data.get('accused_id'), # Nullable
            item_data.get('person_id'),  # Nullable
            item_data.get('existing_accused', False),
            
            item_data.get('full_name'),
            alias_name,
            age,  # Use validated age
            gender,
            occupation,
            item_data.get('address'),
            phone_numbers,
            
            item_data.get('role_in_crime'),
            item_data.get('key_details'),
            item_data.get('accused_type'),
            item_data.get('status'),
            item_data.get('is_ccl', False),
            
            json.dumps(item_data.get('source_person_fields', {})),
            json.dumps(item_data.get('source_accused_fields', {})),
            json.dumps(item_data.get('source_summary_fields', {}))
        ))
    conn.commit()


#!/usr/bin/env python3
"""
Combined Section Processing and Classification Script
Combines logic from logic-1.py and logic-2.py for database processing.

Process:
1. Connect to database using .env variables
2. Process chunks of crimes in parallel
3. Clean NDPS sections (logic-1: extract and normalize)
4. Classify sections (logic-2: categorize into Small/Intermediate/Commercial/Cultivation)
5. Update class_classification in database using executemany/batch
"""

import os
import re
import csv
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from dotenv import load_dotenv
from typing import Optional, List, Dict, Tuple
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Enable importing db_pooling from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_pooling import PostgreSQLConnectionPool

# Load environment variables from .env file
load_dotenv()


class NDPSSectionCleaner:
    """Handles cleaning and extraction of NDPS sections (from logic-1.py)"""
    
    def __init__(self):
        # Regex pattern for NDPSA / NDPSAA sections
        self.ndps_pattern = re.compile(
            r'(\d+[A-Za-z]?(?:-[A-Za-z0-9]+)*)'
            r'((?:\([A-Za-z0-9]+\))*)'
            r'\s*NDPSA{1,2}\b',
            re.IGNORECASE
        )
    
    def extract_sections_as_entities(self, text: Optional[str]) -> List[str]:
        """
        Extract all sections as separate entities from acts_sections.
        - Handles NDPSA/NDPSAA anywhere in the string (before/after/middle)
        - Handles comma-separated sections and "r/w" (read with) patterns
        - Each section is treated as a separate entity for classification
        
        Args:
            text: Raw sections text from database
            
        Returns:
            List of individual section entities (cleaned and normalized)
        """
        if not isinstance(text, str) or not text:
            return []
        
        # Remove "r/w" (read with) - case insensitive
        text = re.sub(r'\br/w\b', ' ', text, flags=re.IGNORECASE)
        
        # Pattern to capture section tokens (supports hyphens + brackets)
        section_pattern = re.compile(
            r'\d+[A-Za-z]?(?:-[A-Za-z0-9]+)*(?:\([A-Za-z0-9]+\))*',
            re.IGNORECASE
        )
        
        entities = []
        
        # Split by comma to get individual sections
        parts = [part.strip() for part in text.split(',') if part.strip()]
        
        for part in parts:
            # Remove any NDPSA/NDPSAA tokens anywhere in the part
            cleaned_part = re.sub(r'\bNDPSA{1,2}\b', ' ', part, flags=re.IGNORECASE)
            
            # Extract section tokens from the cleaned part
            for match in section_pattern.finditer(cleaned_part):
                token = match.group(0)
                token = re.sub(r"-", "", token)       # remove hyphens: 20b-2a -> 20b2a
                token = re.sub(r"[()]", "", token)    # remove brackets: (b)(ii) -> bii
                token = token.lower()
                entities.append(token)
            
            # Fallback: capture standalone numbers (pure numeric sections)
            standalone_numbers = re.findall(r'\b(\d+)\b', cleaned_part)
            for num in standalone_numbers:
                entities.append(num)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_entities = []
        for entity in entities:
            if entity not in seen:
                seen.add(entity)
                unique_entities.append(entity)
        
        return unique_entities
    
    def clean_ndps_sections(self, text: Optional[str]) -> str:
        """
        Extract and clean NDPS sections from text (legacy method for compatibility).
        Normalizes: 27-A -> 27a, 20(b)(ii)(c) -> 20biiic
        
        Args:
            text: Raw sections text from database
            
        Returns:
            Comma-separated cleaned section strings
        """
        entities = self.extract_sections_as_entities(text)
        return ", ".join(entities)


class SectionClassifier:
    """Handles classification of cleaned sections (from logic-2.py)"""
    
    # Priority for final row-level category
    CATEGORY_PRIORITY = {
        "small": 0,
        "intermediate": 1,
        "commercial": 2,
        "cultivation": 3,
    }
    
    @staticmethod
    def normalize_item(item: str) -> str:
        """
        Clean up a section string:
        - strip spaces
        - lowercase
        - remove all non-alphanumeric characters
        
        Args:
            item: Raw section string (e.g., "27a", "20(b)(ii)(c)")
            
        Returns:
            Normalized string (e.g., "27a", "20biic")
        """
        s = str(item).strip().lower()
        s = re.sub(r'[^0-9a-z]', '', s)
        return s
    
    @staticmethod
    def is_numbers_only(section: str) -> bool:
        """
        Check if a section contains only numbers (no letters).
        
        Args:
            section: Section string to check
            
        Returns:
            True if section contains only numbers, False otherwise
        """
        if not section:
            return False
        # Remove all non-alphanumeric characters and check if only digits remain
        normalized = re.sub(r'[^0-9a-z]', '', section.lower())
        # Check if it contains only digits (no letters)
        return bool(re.match(r'^\d+$', normalized))
    
    @staticmethod
    def classify_item(raw_item: str) -> Optional[str]:
        """
        Classify a single cleaned section item into:
        cultivation / commercial / intermediate / small / None
        
        Rules (in order):
        1. If section contains ONLY numbers (no letters) -> small
        2. If 20a is present in any form -> cultivation
        3. If 27 is present (27, 27a, 27b, 27c...) -> small
        4. If exactly 8c -> small
        5. Else:
            * If last letter is a/b/c -> use that as the category marker
            * Otherwise:
                - If A/a is present -> small
                - Else if B/b is present -> intermediate
                - Else if C/c is present -> commercial
        
        Args:
            raw_item: Raw section string
            
        Returns:
            Classification string or None
        """
        code = SectionClassifier.normalize_item(raw_item)
        if not code:
            return None
        
        # NEW RULE: If section contains ONLY numbers (no letters) -> small
        if SectionClassifier.is_numbers_only(raw_item):
            return "small"
        
        # Rule: If 8c, classify as small
        if code == "8c":
            return "small"
        
        # Rule: If 20a is present in any form, always cultivation
        if "20a" in code:
            return "cultivation"
        
        # Rule: If 27 is present, always small (27, 27a, 27b, 27c...)
        if re.match(r"^27[0-9a-z]*$", code):
            return "small"
        
        # General A/B/C logic
        letters = re.findall(r"[a-z]", code)
        if not letters:
            return None
        
        last = letters[-1]
        
        # Case 1: last alphabet is a/b/c -> treat that as the main category marker
        if last in ("a", "b", "c"):
            main_letter = last
        else:
            # Case 2: no trailing a/b/c -> use presence rules
            has_a = "a" in letters
            has_b = "b" in letters
            has_c = "c" in letters
            
            if has_a:
                main_letter = "a"
            elif has_b:
                main_letter = "b"
            elif has_c:
                main_letter = "c"
            else:
                return None
        
        # Map main_letter to category
        if main_letter == "a":
            return "small"
        if main_letter == "b":
            return "intermediate"
        if main_letter == "c":
            return "commercial"
        
        return None
    
    def classify_entities(self, entities: List[str]) -> Tuple[Optional[str], List[Dict[str, str]]]:
        """
        Classify multiple section entities and return final classification with details.
        Each entity is classified individually, then highest priority is selected.
        
        Args:
            entities: List of section entities to classify
            
        Returns:
            Tuple of (final_classification, entity_details)
            entity_details: List of dicts with 'entity', 'classification' for each entity
        """
        if not entities:
            return None, []
        
        entity_details = []
        categories = []
        
        for entity in entities:
            classification = self.classify_item(entity)
            entity_details.append({
                'entity': entity,
                'classification': classification or 'None'
            })
            if classification is not None:
                categories.append(classification)
        
        if not categories:
            return None, entity_details
        
        # Pick the category with highest priority
        best = max(categories, key=lambda c: self.CATEGORY_PRIORITY[c])
        
        # Nicely capitalised: "Small", "Intermediate", etc.
        return best.capitalize(), entity_details
    
    def classify_row(self, value: str) -> Optional[str]:
        """
        Classify a full row (multiple comma-separated items in CleanedSections).
        Returns a single final category for the row based on priority:
        Cultivation > Commercial > Intermediate > Small
        
        Args:
            value: Comma-separated cleaned sections string
            
        Returns:
            Classification string (capitalized) or None
        """
        if not value or not value.strip():
            return None
        
        text = str(value)
        items = [item.strip() for item in text.split(",") if item.strip()]
        
        categories = []
        for item in items:
            cat = self.classify_item(item)
            if cat is not None:
                categories.append(cat)
        
        if not categories:
            return None
        
        # Pick the category with highest priority
        best = max(categories, key=lambda c: self.CATEGORY_PRIORITY[c])
        
        # Nicely capitalised: "Small", "Intermediate", etc.
        return best.capitalize()


def check_column_exists(cursor) -> bool:
    """Check if class_classification column exists in crimes table"""
    try:
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'crimes' AND column_name = 'class_classification'
        """)
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"✗ Error checking column existence: {e}")
        raise


def create_column(cursor, conn):
    """Create class_classification column if it doesn't exist"""
    try:
        cursor.execute("""
            ALTER TABLE crimes 
            ADD COLUMN class_classification VARCHAR(50)
        """)
        conn.commit()
        print("  ✓ Column 'class_classification' created successfully")
    except Exception as e:
        conn.rollback()
        print(f"  ✗ Error creating column: {e}")
        raise


def process_crime_chunk(chunk: List[Dict], cleaner: NDPSSectionCleaner, classifier: SectionClassifier, db_pool: PostgreSQLConnectionPool):
    """Process a chunk of crimes: extract, classify, and update db."""
    logic1_data = []
    logic2_data = []
    logic2_entity_details = []
    
    stats = {
        'total': 0, 'updated': 0, 'no_change': 0, 'null_sections': 0,
        'no_match': 0, 'cultivation': 0, 'commercial': 0,
        'intermediate': 0, 'small': 0, 'null_classification': 0,
        'errors': 0
    }
    
    update_batch = []
    
    for crime in chunk:
        try:
            crime_id = crime['crime_id']
            sections_text = crime['acts_sections']
            existing_classification = crime.get('class_classification')
            
            stats['total'] += 1
            
            # Phase 1: Extract Entities
            entities = cleaner.extract_sections_as_entities(sections_text)
            entities_str = ", ".join(entities) if entities else ""
            
            logic1_data.append({
                'crime_id': crime_id,
                'acts_sections': sections_text or '',
                'entities': entities_str,
                'entity_count': len(entities)
            })
            
            # Phase 2: Classify Entities
            classification, entity_details = classifier.classify_entities(entities)
            
            for detail in entity_details:
                logic2_entity_details.append({
                    'crime_id': crime_id,
                    'entity': detail['entity'],
                    'entity_classification': detail['classification']
                })
                
            logic2_data.append({
                'crime_id': crime_id,
                'entities': entities_str,
                'entity_count': len(entities),
                'class_classification': classification or ''
            })
            
            # Update stats based on classification
            if not sections_text or not sections_text.strip():
                stats['null_sections'] += 1
            elif classification:
                stats[classification.lower()] += 1
            else:
                stats['no_match'] += 1
                stats['null_classification'] += 1
                
            # Phase 3: Prepare DB Update
            if existing_classification != classification:
                update_batch.append((classification, crime_id))
                stats['updated'] += 1
            else:
                stats['no_change'] += 1
                
        except Exception as e:
            print(f"Error processing crime_id {crime.get('crime_id', 'Unknown')}: {e}")
            stats['errors'] += 1
            
    # Execute batch update using connection from pool
    if update_batch:
        try:
            with db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                # Batch update
                execute_batch(
                    cursor,
                    "UPDATE crimes SET class_classification = %s WHERE crime_id = %s",
                    update_batch,
                    page_size=100
                )
                conn.commit()
        except Exception as e:
            print(f"Error updating batch in DB: {e}")
            stats['errors'] += len(update_batch)
            stats['updated'] -= len(update_batch)
            
    return logic1_data, logic2_data, logic2_entity_details, stats


def process_sections():
    print("=" * 80)
    print("Section Processing and Classification Script (Parallelized)")
    print("=" * 80)
    
    cleaner = NDPSSectionCleaner()
    classifier = SectionClassifier()
    
    print("\n[STEP 1] Initializing Database Connection Pool...")
    try:
        db_pool = PostgreSQLConnectionPool(minconn=1, maxconn=10)
        with db_pool.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            print("  ✓ Connected successfully")
            print(f"  → Host: {os.getenv('DB_HOST')}")
            print(f"  → Database: {os.getenv('DB_NAME')}")
            
            print("\n[STEP 2] Checking column existence...")
            column_exists = check_column_exists(cursor)
            
            if not column_exists:
                print("  → Column 'class_classification' does not exist. Creating...")
                create_column(cursor, conn)
            else:
                print("  ✓ Column 'class_classification' already exists")
                
            print("\n[STEP 3] Fetching crime records...")
            cursor.execute("SELECT COUNT(*) as count FROM crimes")
            total_records = cursor.fetchone()['count']
            print(f"  ✓ Found {total_records} crime records to process")
            
            cursor.execute("SELECT crime_id, acts_sections, class_classification FROM crimes ORDER BY crime_id")
            crimes = cursor.fetchall()
            print(f"  ✓ Retrieved {len(crimes)} records")
            
    except Exception as e:
        print(f"  ✗ Failed to initialize: {e}")
        return
        
    print("\n[STEP 4] Processing Records Concurrently...")
    
    # Chunking
    chunk_size = 5000
    chunks = [crimes[i:i + chunk_size] for i in range(0, len(crimes), chunk_size)]
    print(f"  → Created {len(chunks)} chunks of size {chunk_size}")
    
    all_logic1_data = []
    all_logic2_data = []
    all_logic2_entity_details = []
    
    global_stats = {
        'total': 0, 'updated': 0, 'no_change': 0, 'null_sections': 0,
        'no_match': 0, 'cultivation': 0, 'commercial': 0,
        'intermediate': 0, 'small': 0, 'null_classification': 0,
        'errors': 0
    }
    
    # Get max workers from env or default to 5
    max_workers = int(os.getenv('MAX_WORKERS', '5'))
    print(f"  → Using ThreadPoolExecutor with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_crime_chunk, chunk, cleaner, classifier, db_pool): i for i, chunk in enumerate(chunks)}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                l1_data, l2_data, l2_ent, stats = future.result()
                all_logic1_data.extend(l1_data)
                all_logic2_data.extend(l2_data)
                all_logic2_entity_details.extend(l2_ent)
                
                for k, v in stats.items():
                    global_stats[k] += v
                    
                if i % 10 == 0 or i == len(chunks):
                    print(f"  ... processed {i}/{len(chunks)} chunks")
            except Exception as e:
                print(f"✗ Chunk processing failed: {e}")
                
    # Save CSVs
    print("\n[STEP 5] Saving Results to CSV...")
    logic1_output_file = "logic1_output.csv"
    with open(logic1_output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['crime_id', 'acts_sections', 'entities', 'entity_count'])
        writer.writeheader()
        writer.writerows(all_logic1_data)
    print(f"  ✓ Saved {len(all_logic1_data)} records to {logic1_output_file}")
    
    logic2_output_file = "logic2_output.csv"
    with open(logic2_output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['crime_id', 'entities', 'entity_count', 'class_classification'])
        writer.writeheader()
        writer.writerows(all_logic2_data)
    print(f"  ✓ Saved {len(all_logic2_data)} records to {logic2_output_file}")
    
    logic2_entity_details_file = "logic2_entity_details.csv"
    with open(logic2_entity_details_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['crime_id', 'entity', 'entity_classification'])
        writer.writeheader()
        writer.writerows(all_logic2_entity_details)
    print(f"  ✓ Saved {len(all_logic2_entity_details)} entity details to {logic2_entity_details_file}")
    
    # Close pool
    if hasattr(db_pool, 'close_all'):
        db_pool.close_all()
    
    # Print summary
    print("\n" + "-" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total Records Processed:      {global_stats['total']}")
    print(f"Records Updated:             {global_stats['updated']}")
    print(f"Records (No Change):         {global_stats['no_change']}")
    print(f"Processing Errors:           {global_stats['errors']}")
    print(f"\nClassification Breakdown:")
    print(f"  - Cultivation:              {global_stats['cultivation']}")
    print(f"  - Commercial:               {global_stats['commercial']}")
    print(f"  - Intermediate:             {global_stats['intermediate']}")
    print(f"  - Small:                    {global_stats['small']}")
    print(f"  - NULL (No Match):          {global_stats['null_classification']}")
    print(f"\nOther Statistics:")
    print(f"  - NULL/Empty acts_sections: {global_stats['null_sections']}")
    print("=" * 80)
    print("\n✓ Section processing and classification completed successfully!")


if __name__ == "__main__":
    try:
        process_sections()
    except KeyboardInterrupt:
        print("\n\n✗ Script interrupted by user")
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        import traceback
        traceback.print_exc()

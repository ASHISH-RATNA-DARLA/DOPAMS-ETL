#!/usr/bin/env python3
"""
Combined Section Processing and Classification Script
Combines logic from logic-1.py and logic-2.py for database processing.

Process:
1. Connect to database using .env variables
2. Process one section at a time from crimes table
3. Clean NDPS sections (logic-1: extract and normalize)
4. Classify sections (logic-2: categorize into Small/Intermediate/Commercial/Cultivation)
5. Update class_classification in database
"""

import os
import re
import csv
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from typing import Optional, List, Dict, Tuple

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


def get_db_connection():
    """Create and return a database connection using environment variables from .env"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"✗ Error connecting to database: {e}")
        raise


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


def process_sections():
    """Main function to process sections from database and update classifications"""
    
    print("=" * 80)
    print("Section Processing and Classification Script")
    print("Combined logic from logic-1.py and logic-2.py")
    print("=" * 80)
    
    # Initialize components
    cleaner = NDPSSectionCleaner()
    classifier = SectionClassifier()
    
    # Connect to database
    print("\n[STEP 1] Connecting to database...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        print("  ✓ Connected successfully")
        print(f"  → Host: {os.getenv('DB_HOST')}")
        print(f"  → Database: {os.getenv('DB_NAME')}")
    except Exception as e:
        print(f"  ✗ Failed to connect: {e}")
        return
    
    try:
        # Check and create column if needed
        print("\n[STEP 2] Checking column existence...")
        column_exists = check_column_exists(cursor)
        
        if not column_exists:
            print("  → Column 'class_classification' does not exist")
            print("  → Creating column...")
            create_column(cursor, conn)
        else:
            print("  ✓ Column 'class_classification' already exists")
        
        # Get total count
        print("\n[STEP 3] Counting records...")
        cursor.execute("SELECT COUNT(*) as count FROM crimes")
        total_records = cursor.fetchone()['count']
        print(f"  ✓ Found {total_records} crime records to process")
        
        # Fetch all crime records
        print("\n[STEP 4] Fetching crime records...")
        cursor.execute("""
            SELECT crime_id, acts_sections, class_classification 
            FROM crimes 
            ORDER BY crime_id
        """)
        crimes = cursor.fetchall()
        print(f"  ✓ Retrieved {len(crimes)} records")
        
        # ============================================================
        # PHASE 1: LOGIC-1 - Extract Sections as Entities
        # ============================================================
        print("\n" + "=" * 80)
        print("[PHASE 1] LOGIC-1: Extracting Sections as Entities")
        print("=" * 80)
        print("-" * 80)
        
        logic1_output_file = "logic1_output.csv"
        logic1_data = []  # Store data for CSV
        
        for idx, crime in enumerate(crimes, 1):
            crime_id = crime['crime_id']
            sections_text = crime['acts_sections']
            
            # Log every 100 records or for first 10 records
            log_detail = (idx % 100 == 0) or (idx <= 10)
            
            if log_detail:
                print(f"\n[{idx}/{total_records}] Crime ID: {crime_id}")
                print(f"  [EXTRACT] Original sections: {sections_text}")
            
            # Step 1: Extract sections as entities (logic-1)
            entities = cleaner.extract_sections_as_entities(sections_text)
            entities_str = ", ".join(entities) if entities else ""
            
            if log_detail:
                print(f"  [EXTRACT] Entities found: {len(entities)}")
                print(f"  [EXTRACT] Entities: {entities_str}")
            
            # Store data for CSV
            logic1_data.append({
                'crime_id': crime_id,
                'acts_sections': sections_text or '',
                'entities': entities_str,
                'entity_count': len(entities)
            })
        
        # Save Logic-1 results to CSV
        print(f"\n[PHASE 1] Saving Logic-1 results to {logic1_output_file}...")
        with open(logic1_output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['crime_id', 'acts_sections', 'entities', 'entity_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(logic1_data)
        print(f"  ✓ Saved {len(logic1_data)} records to {logic1_output_file}")
        
        # ============================================================
        # PHASE 2: LOGIC-2 - Classify Each Entity
        # ============================================================
        print("\n" + "=" * 80)
        print("[PHASE 2] LOGIC-2: Classifying Each Entity")
        print("=" * 80)
        print("-" * 80)
        
        logic2_output_file = "logic2_output.csv"
        logic2_data = []  # Store data for CSV
        logic2_entity_details_file = "logic2_entity_details.csv"
        logic2_entity_details = []  # Store entity-level details
        
        # Statistics
        stats = {
            'total': 0,
            'updated': 0,
            'no_change': 0,
            'null_sections': 0,
            'no_match': 0,
            'cultivation': 0,
            'commercial': 0,
            'intermediate': 0,
            'small': 0,
            'null_classification': 0
        }
        
        for idx, logic1_record in enumerate(logic1_data, 1):
            crime_id = logic1_record['crime_id']
            entities_str = logic1_record['entities']
            
            # Parse entities from string
            entities = [e.strip() for e in entities_str.split(',') if e.strip()] if entities_str else []
            
            # Get original crime record for existing classification
            original_crime = next((c for c in crimes if c['crime_id'] == crime_id), None)
            existing_classification = original_crime.get('class_classification') if original_crime else None
            
            stats['total'] += 1
            
            # Log every 100 records or for first 10 records
            log_detail = (idx % 100 == 0) or (idx <= 10)
            
            if log_detail:
                print(f"\n[{idx}/{total_records}] Crime ID: {crime_id}")
                print(f"  [CLASSIFY] Entities: {entities_str}")
            
            # Step 2: Classify each entity individually (logic-2)
            classification, entity_details = classifier.classify_entities(entities)
            
            if log_detail:
                print(f"  [CLASSIFY] Entity details:")
                for detail in entity_details:
                    print(f"    - Entity: {detail['entity']} → {detail['classification']}")
                print(f"  [CLASSIFY] Final Classification: {classification or 'NULL'}")
            
            # Store entity-level details for CSV
            for detail in entity_details:
                logic2_entity_details.append({
                    'crime_id': crime_id,
                    'entity': detail['entity'],
                    'entity_classification': detail['classification']
                })
            
            # Store summary data for CSV
            logic2_data.append({
                'crime_id': crime_id,
                'entities': entities_str,
                'entity_count': len(entities),
                'class_classification': classification or ''
            })
            
            # Track statistics
            original_sections = original_crime['acts_sections'] if original_crime else None
            if not original_sections or not original_sections.strip():
                stats['null_sections'] += 1
            elif classification:
                stats[classification.lower()] += 1
            else:
                stats['no_match'] += 1
                stats['null_classification'] += 1
        
        # Save Logic-2 summary results to CSV
        print(f"\n[PHASE 2] Saving Logic-2 summary results to {logic2_output_file}...")
        with open(logic2_output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['crime_id', 'entities', 'entity_count', 'class_classification']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(logic2_data)
        print(f"  ✓ Saved {len(logic2_data)} records to {logic2_output_file}")
        
        # Save Logic-2 entity-level details to CSV
        print(f"\n[PHASE 2] Saving Logic-2 entity details to {logic2_entity_details_file}...")
        with open(logic2_entity_details_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['crime_id', 'entity', 'entity_classification']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(logic2_entity_details)
        print(f"  ✓ Saved {len(logic2_entity_details)} entity details to {logic2_entity_details_file}")
        
        # ============================================================
        # PHASE 3: Update Database
        # ============================================================
        print("\n" + "=" * 80)
        print("[PHASE 3] Updating Database")
        print("=" * 80)
        print("-" * 80)
        
        for idx, logic2_record in enumerate(logic2_data, 1):
            crime_id = logic2_record['crime_id']
            classification = logic2_record['class_classification']
            
            # Get original crime record for existing classification
            original_crime = next((c for c in crimes if c['crime_id'] == crime_id), None)
            existing_classification = original_crime.get('class_classification') if original_crime else None
            
            # Log every 100 records or for first 10 records
            log_detail = (idx % 100 == 0) or (idx <= 10)
            
            if log_detail:
                print(f"\n[{idx}/{total_records}] Crime ID: {crime_id}")
            
            # Update database
            if existing_classification:
                if existing_classification == classification:
                    # Already correct, no change needed
                    stats['no_change'] += 1
                    if log_detail:
                        print(f"  [VERIFY] ✓ No change needed (already {classification})")
                else:
                    # Classification is different, update it
                    cursor.execute("""
                        UPDATE crimes 
                        SET class_classification = %s 
                        WHERE crime_id = %s
                    """, (classification, crime_id))
                    stats['updated'] += 1
                    if log_detail:
                        print(f"  [UPDATE] ✗ Updated from '{existing_classification}' to '{classification or 'NULL'}'")
            else:
                # No existing classification, set it
                cursor.execute("""
                    UPDATE crimes 
                    SET class_classification = %s 
                    WHERE crime_id = %s
                """, (classification, crime_id))
                stats['updated'] += 1
                if log_detail:
                    print(f"  [UPDATE] ✓ Set classification to '{classification or 'NULL'}'")
            
            # Commit every 100 records for safety
            if idx % 100 == 0:
                conn.commit()
                if log_detail:
                    print(f"  [COMMIT] Progress saved (processed {idx} records)")
        
        # Final commit
        conn.commit()
        print("\n  ✓ Database update completed")
        
        # Print summary
        print("\n" + "-" * 80)
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total Records Processed:      {stats['total']}")
        print(f"Records Updated:             {stats['updated']}")
        print(f"Records (No Change):         {stats['no_change']}")
        print(f"\nClassification Breakdown:")
        print(f"  - Cultivation:              {stats['cultivation']}")
        print(f"  - Commercial:              {stats['commercial']}")
        print(f"  - Intermediate:             {stats['intermediate']}")
        print(f"  - Small:                    {stats['small']}")
        print(f"  - NULL (No Match):           {stats['null_classification']}")
        print(f"\nOther Statistics:")
        print(f"  - NULL/Empty acts_sections:  {stats['null_sections']}")
        print(f"\nOutput Files Generated:")
        print(f"  - {logic1_output_file} (Logic-1: Entity extraction results)")
        print(f"  - {logic2_output_file} (Logic-2: Summary classification results)")
        print(f"  - {logic2_entity_details_file} (Logic-2: Entity-level classification details)")
        print("=" * 80)
        print("\n✓ Section processing and classification completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cursor.close()
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    try:
        process_sections()
    except KeyboardInterrupt:
        print("\n\n✗ Script interrupted by user")
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        import traceback
        traceback.print_exc()



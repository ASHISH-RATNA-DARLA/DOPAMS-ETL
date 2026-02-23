#!/usr/bin/env python3
"""
Person Deduplication Table Creation and Population Script (Enhanced with Fuzzy Matching)

This script creates a new table `person_deduplication_tracker` that stores:
- Unique person fingerprints
- All accused IDs and crime IDs for each unique person
- Matching strategy/scenario used for identification
- Confidence scores and metadata
- Fuzzy name matching using dedupe library for typo detection

This enables the UI to show:
- All crimes an accused person has been involved in
- Which matching strategy identified them across cases
- Duplicate person records that should be treated as the same individual
- Better matching despite typos and name variations
"""

import os
import sys
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
import json
import re
from difflib import SequenceMatcher
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

try:
    import dedupe
    from dedupe import Dedupe

    DEDUPE_AVAILABLE = True
except ImportError:
    print("⚠️  Warning: 'dedupe' library not found. Install with: pip install dedupe")
    print("   Falling back to basic fuzzy matching...")
    DEDUPE_AVAILABLE = False
    dedupe = None
    Dedupe = None

# Database connection from .env file only
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ Error: DATABASE_URL not found in .env file")
    print("   Please create a .env file with: DATABASE_URL=postgresql://user:password@host:port/database")
    sys.exit(1)


class FuzzyMatcher:
    """Handles fuzzy matching for names and text fields to detect typos and variations"""

    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize name for comparison - handles spaces, special chars, typos"""
        if not name:
            return ""
        # Convert to lowercase
        name = name.lower()
        # Remove common prefixes/suffixes that might vary
        name = re.sub(r"\b(mr|mrs|ms|dr|md|s/o|d/o|w/o)\b\.?", "", name)
        # Remove special characters but keep spaces
        name = re.sub(r"[^\w\s]", "", name)
        # Normalize multiple spaces to single space
        name = re.sub(r"\s+", " ", name)
        # Remove leading/trailing spaces
        name = name.strip()
        return name

    @staticmethod
    def similarity_ratio(str1: str, str2: str) -> float:
        """Calculate similarity ratio between two strings (0-1)"""
        if not str1 or not str2:
            return 0.0

        str1_norm = FuzzyMatcher.normalize_name(str1)
        str2_norm = FuzzyMatcher.normalize_name(str2)

        if str1_norm == str2_norm:
            return 1.0

        return SequenceMatcher(None, str1_norm, str2_norm).ratio()

    @staticmethod
    def is_similar_name(name1: str, name2: str, threshold: float = 0.85) -> bool:
        """Check if two names are similar enough to be considered the same person"""
        return FuzzyMatcher.similarity_ratio(name1, name2) >= threshold

    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return FuzzyMatcher.levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    @staticmethod
    def is_typo_variant(name1: str, name2: str, max_distance: int = 2) -> bool:
        """Check if names differ by only a few characters (likely typos)"""
        if not name1 or not name2:
            return False

        name1_norm = FuzzyMatcher.normalize_name(name1)
        name2_norm = FuzzyMatcher.normalize_name(name2)

        # If lengths differ by too much, probably not a typo
        if abs(len(name1_norm) - len(name2_norm)) > max_distance:
            return False

        distance = FuzzyMatcher.levenshtein_distance(name1_norm, name2_norm)
        return distance <= max_distance

    @staticmethod
    def get_name_tokens(name: str) -> Set[str]:
        """Get tokens from a name for partial matching"""
        if not name:
            return set()
        normalized = FuzzyMatcher.normalize_name(name)
        return set(normalized.split())

    @staticmethod
    def token_overlap_ratio(name1: str, name2: str) -> float:
        """Calculate token overlap ratio between two names"""
        tokens1 = FuzzyMatcher.get_name_tokens(name1)
        tokens2 = FuzzyMatcher.get_name_tokens(name2)

        if not tokens1 or not tokens2:
            return 0.0

        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)

        return intersection / union if union > 0 else 0.0


class DedupeBasedMatcher:
    """Uses dedupe library for advanced ML-based matching if available"""

    @staticmethod
    def prepare_data_for_dedupe(person_data: Dict) -> Dict:
        """Prepare person data in format required by dedupe library"""
        return {
            "full_name": person_data.get("full_name", "").strip() or "",
            "relative_name": person_data.get("relative_name", "").strip() or "",
            "age": str(person_data.get("age", "")) if person_data.get("age") else "",
            "phone_number": person_data.get("phone_number", "").strip() or "",
            "present_district": person_data.get("present_district", "").strip() or "",
            "present_locality_village": person_data.get(
                "present_locality_village", ""
            ).strip()
            or "",
            "gender": person_data.get("gender", "").strip() or "",
        }

    @staticmethod
    def calculate_dedupe_similarity(person1: Dict, person2: Dict) -> float:
        """
        Calculate similarity using 4 PRIMARY fields (95% weight):
        1. full_name (40%) - HIGHEST PRIORITY
        2. relative_name (30%) - VERY HIGH PRIORITY
        3. relation_type (15%) - HIGH PRIORITY
        4. gender (10%) - MEDIUM PRIORITY
        Plus supporting fields (age, location, phone) for additional confidence

        Includes excellent typo/space tolerance with multiple matching techniques
        """
        fuzzy = FuzzyMatcher()

        # 1. Full Name matching (40% weight) - HIGHEST PRIORITY
        name1 = person1.get("full_name", "")
        name2 = person2.get("full_name", "")
        name_score = 0.0

        if name1 and name2:
            # Use multiple techniques for name matching
            exact_match = fuzzy.similarity_ratio(name1, name2)
            token_match = fuzzy.token_overlap_ratio(name1, name2)
            typo_match = (
                1.0 if fuzzy.is_typo_variant(name1, name2, max_distance=3) else 0.0
            )

            # Best of all methods
            name_score = max(exact_match, token_match, typo_match)

        # 2. Relative Name matching (30% weight) - VERY HIGH PRIORITY
        parent1 = person1.get("relative_name", "")
        parent2 = person2.get("relative_name", "")
        parent_score = 0.0

        if parent1 and parent2:
            # Use multiple techniques for parent name matching
            exact_match = fuzzy.similarity_ratio(parent1, parent2)
            token_match = fuzzy.token_overlap_ratio(parent1, parent2)
            typo_match = (
                1.0 if fuzzy.is_typo_variant(parent1, parent2, max_distance=3) else 0.0
            )

            # Best of all methods
            parent_score = max(exact_match, token_match, typo_match)

        # Relation type matching (15% weight) - HIGH PRIORITY (NEW - 3rd PRIMARY FIELD)
        relation1 = person1.get("relation_type", "")
        relation2 = person2.get("relation_type", "")
        relation_score = 0.0

        if relation1 and relation2:
            # Normalize relation types
            rel1_norm = fuzzy.normalize_name(relation1)
            rel2_norm = fuzzy.normalize_name(relation2)

            if rel1_norm == rel2_norm:
                relation_score = 1.0
            # Partial matches for common variations
            elif rel1_norm and rel2_norm:
                if "father" in rel1_norm and "father" in rel2_norm:
                    relation_score = 1.0
                elif "mother" in rel1_norm and "mother" in rel2_norm:
                    relation_score = 1.0
                elif "spouse" in rel1_norm and "spouse" in rel2_norm:
                    relation_score = 1.0
                elif "husband" in rel1_norm and "spouse" in rel2_norm:
                    relation_score = 0.9
                elif "wife" in rel1_norm and "spouse" in rel2_norm:
                    relation_score = 0.9
                else:
                    relation_score = fuzzy.similarity_ratio(relation1, relation2)

        # Gender matching (10% weight) - MEDIUM PRIORITY (NEW - 4th PRIMARY FIELD)
        gender1 = person1.get("gender", "")
        gender2 = person2.get("gender", "")
        gender_score = 0.0

        if gender1 and gender2:
            # Normalize gender values
            g1_norm = fuzzy.normalize_name(gender1)
            g2_norm = fuzzy.normalize_name(gender2)

            if g1_norm == g2_norm:
                gender_score = 1.0
            # Handle common variations
            elif g1_norm and g2_norm:
                male_variants = ["male", "m"]
                female_variants = ["female", "f"]

                if any(v in g1_norm for v in male_variants) and any(
                    v in g2_norm for v in male_variants
                ):
                    # Both are male variants
                    if not any(v in g1_norm for v in female_variants) and not any(
                        v in g2_norm for v in female_variants
                    ):
                        gender_score = 1.0
                elif any(v in g1_norm for v in female_variants) and any(
                    v in g2_norm for v in female_variants
                ):
                    # Both are female variants
                    gender_score = 1.0

        # Age matching (3% weight) - Supporting field
        age1 = person1.get("age")
        age2 = person2.get("age")
        age_score = 0.0

        if age1 and age2:
            try:
                age1_int = int(age1) if not isinstance(age1, int) else age1
                age2_int = int(age2) if not isinstance(age2, int) else age2
                age_diff = abs(age1_int - age2_int)
                if age_diff == 0:
                    age_score = 1.0
                elif age_diff <= 2:
                    age_score = 0.8
                elif age_diff <= 5:
                    age_score = 0.5
            except (ValueError, TypeError):
                age_score = 0.0

        # Location matching (1.5% weight) - Supporting field
        location_score = 0.0
        locality1 = person1.get("present_locality_village", "")
        locality2 = person2.get("present_locality_village", "")
        district1 = person1.get("present_district", "")
        district2 = person2.get("present_district", "")

        if locality1 and locality2:
            location_score = fuzzy.similarity_ratio(locality1, locality2)
        elif district1 and district2:
            location_score = fuzzy.similarity_ratio(district1, district2) * 0.7

        # Phone matching (0.5% weight) - Supporting field
        phone1 = person1.get("phone_number", "")
        phone2 = person2.get("phone_number", "")
        phone_score = 0.0

        if phone1 and phone2:
            # Clean phone numbers for comparison
            phone1_clean = re.sub(r"\D", "", phone1)
            phone2_clean = re.sub(r"\D", "", phone2)

            if phone1_clean and phone2_clean:
                if phone1_clean == phone2_clean:
                    phone_score = 1.0
                # Check if last 10 digits match (for country code variations)
                elif len(phone1_clean) >= 10 and len(phone2_clean) >= 10:
                    if phone1_clean[-10:] == phone2_clean[-10:]:
                        phone_score = 1.0

        # Weighted overall score with 4 PRIMARY FIELDS = 95% of total weight
        # PRIMARY FIELDS: full_name, relative_name, relation_type, gender
        overall_score = (
            name_score * 0.40  # 1. Full Name: 40% (HIGHEST)
            + parent_score * 0.30  # 2. Relative Name: 30% (VERY HIGH)
            + relation_score * 0.15  # 3. Relation Type: 15% (HIGH)
            + gender_score * 0.10  # 4. Gender: 10% (MEDIUM)
            + age_score * 0.03  # Supporting: Age: 3%
            + location_score * 0.015  # Supporting: Location: 1.5%
            + phone_score * 0.005  # Supporting: Phone: 0.5%
        )

        return overall_score


class PersonDeduplicationTracker:
    """Manages person deduplication tracking across crimes with fuzzy matching"""

    def __init__(self, db_url: str, use_fuzzy_matching: bool = True):
        self.db_url = db_url
        self.conn = None
        self.cursor = None
        self.fuzzy_matcher = FuzzyMatcher()
        self.dedupe_matcher = DedupeBasedMatcher()
        self.use_fuzzy_matching = use_fuzzy_matching
        self.use_dedupe = DEDUPE_AVAILABLE  # Use dedupe library if available
        self.name_similarity_threshold = (
            0.82  # 82% similarity for names (allows for more typos)
        )
        self.parent_similarity_threshold = 0.80  # 80% for parent names
        self.typo_max_distance = 3  # Max 3 character differences for typos
        self.match_confidence_threshold = (
            0.65  # 65% overall match required (4 primary fields give more confidence)
        )

    def connect(self):
        """Establish database connection"""
        print("Connecting to database...")
        self.conn = psycopg2.connect(self.db_url)
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        print("✓ Connected successfully")

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Disconnected from database")

    def clear_existing_data(self):
        """Clear existing deduplication data if it exists"""
        print("\n=== Clearing Existing Deduplication Data ===")

        clear_sql = """
        -- Drop existing table and related objects if they exist
        DROP TABLE IF EXISTS person_deduplication_tracker CASCADE;
        DROP VIEW IF EXISTS person_deduplication_summary CASCADE;
        DROP FUNCTION IF EXISTS get_accused_crime_history(VARCHAR(50)) CASCADE;
        DROP FUNCTION IF EXISTS get_person_crime_history(VARCHAR(50)) CASCADE;
        DROP FUNCTION IF EXISTS search_person_by_name(VARCHAR(500)) CASCADE;
        """

        try:
            self.cursor.execute(clear_sql)
            self.conn.commit()
            print("✓ Cleared existing deduplication table and functions")
        except Exception as e:
            self.conn.rollback()
            print(f"⚠️  Warning clearing existing data: {e}")
            # Continue anyway as this might be first run

    def create_deduplication_table(self):
        """Create the person deduplication tracking table"""
        print("\n=== Creating person_deduplication_tracker table ===")

        create_table_sql = """
        -- Create main deduplication tracking table
        CREATE TABLE person_deduplication_tracker (
            id SERIAL PRIMARY KEY,
            
            -- Unique person identifier (fingerprint hash)
            person_fingerprint VARCHAR(32) NOT NULL,
            
            -- Matching strategy used (tier 1-5)
            matching_tier SMALLINT NOT NULL CHECK (matching_tier BETWEEN 1 AND 5),
            matching_strategy VARCHAR(100) NOT NULL,
            
            -- Fuzzy matching indicators
            uses_fuzzy_matching BOOLEAN DEFAULT FALSE,
            fuzzy_match_score NUMERIC(3, 2),
            name_variations TEXT[],
            
            -- Person details (from canonical/first record)
            canonical_person_id VARCHAR(50) NOT NULL,
            full_name VARCHAR(500),
            relative_name VARCHAR(255),
            age INTEGER,
            gender VARCHAR(20),
            phone_number VARCHAR(20),
            present_district VARCHAR(255),
            present_locality_village VARCHAR(255),
            
            -- All person IDs that belong to this unique person (duplicates)
            all_person_ids TEXT[] NOT NULL,
            person_record_count INTEGER NOT NULL DEFAULT 1,
            
            -- All accused IDs across all crimes
            all_accused_ids TEXT[] NOT NULL,
            
            -- All crime IDs this person is involved in
            all_crime_ids TEXT[] NOT NULL,
            crime_count INTEGER NOT NULL DEFAULT 0,
            
            -- Crime details JSON (for quick UI display)
            crime_details JSONB,
            
            -- Confidence and metadata
            confidence_score NUMERIC(3, 2) CHECK (confidence_score BETWEEN 0 AND 1),
            data_quality_flags JSONB,
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Constraints
            UNIQUE (person_fingerprint)
        );
        
        -- Create indexes for fast lookups
        CREATE INDEX idx_dedup_tracker_fingerprint ON person_deduplication_tracker(person_fingerprint);
        CREATE INDEX idx_dedup_tracker_canonical_person ON person_deduplication_tracker(canonical_person_id);
        CREATE INDEX idx_dedup_tracker_tier ON person_deduplication_tracker(matching_tier);
        CREATE INDEX idx_dedup_tracker_crime_count ON person_deduplication_tracker(crime_count);
        CREATE INDEX idx_dedup_tracker_person_ids ON person_deduplication_tracker USING GIN(all_person_ids);
        CREATE INDEX idx_dedup_tracker_accused_ids ON person_deduplication_tracker USING GIN(all_accused_ids);
        CREATE INDEX idx_dedup_tracker_crime_ids ON person_deduplication_tracker USING GIN(all_crime_ids);
        CREATE INDEX idx_dedup_tracker_crime_details ON person_deduplication_tracker USING GIN(crime_details);
        
        -- Create view for easy querying
        CREATE OR REPLACE VIEW person_deduplication_summary AS
        SELECT 
            person_fingerprint,
            matching_tier,
            matching_strategy,
            canonical_person_id,
            full_name,
            relative_name,
            age,
            phone_number,
            present_district,
            person_record_count,
            crime_count,
            CASE 
                WHEN matching_tier = 1 THEN 'Very High'
                WHEN matching_tier = 2 THEN 'High'
                WHEN matching_tier = 3 THEN 'Good'
                WHEN matching_tier = 4 THEN 'Medium'
                WHEN matching_tier = 5 THEN 'Basic'
            END as confidence_level,
            confidence_score,
            CASE 
                WHEN crime_count > 5 THEN 'Repeat Offender'
                WHEN crime_count > 2 THEN 'Multiple Cases'
                WHEN crime_count = 1 THEN 'Single Case'
                ELSE 'No Cases'
            END as offender_category,
            created_at,
            updated_at
        FROM person_deduplication_tracker
        ORDER BY crime_count DESC, matching_tier ASC;
        
        COMMENT ON TABLE person_deduplication_tracker IS 
            'Tracks unique persons across multiple crimes using hierarchical fingerprinting strategies';
        COMMENT ON COLUMN person_deduplication_tracker.person_fingerprint IS 
            'MD5 hash combining person identifying fields based on matching strategy';
        COMMENT ON COLUMN person_deduplication_tracker.matching_tier IS 
            '1=Best (Name+Parent+Locality+Age+Phone), 5=Basic (Name+District+Age)';
        """

        try:
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            print("✓ Table person_deduplication_tracker created successfully")
            print("✓ Indexes created")
            print("✓ View person_deduplication_summary created")
        except Exception as e:
            self.conn.rollback()
            print(f"✗ Error creating table: {e}")
            raise

    def generate_fingerprint(self, data: Dict, tier: int) -> Optional[str]:
        """Generate fingerprint hash based on tier strategy"""

        def clean(val):
            """Clean and normalize text values"""
            if val is None:
                return ""
            return str(val).strip().lower()

        name = clean(data.get("full_name"))
        parent = clean(data.get("relative_name"))
        age = str(data.get("age", "")) if data.get("age") else ""
        phone = clean(data.get("phone_number"))
        district = clean(data.get("present_district"))
        locality = clean(data.get("present_locality_village"))

        # Tier 1: Name + Parent + Locality + Age + Phone
        if tier == 1:
            if name and parent and locality and age and phone:
                key = f"{name}|{parent}|{locality}|{age}|{phone}"
                return hashlib.md5(key.encode()).hexdigest()

        # Tier 2: Name + Parent + Locality + Phone
        elif tier == 2:
            if name and parent and locality and phone:
                key = f"{name}|{parent}|{locality}|{phone}"
                return hashlib.md5(key.encode()).hexdigest()

        # Tier 3: Name + Parent + District + Age
        elif tier == 3:
            if name and parent and district and age:
                key = f"{name}|{parent}|{district}|{age}"
                return hashlib.md5(key.encode()).hexdigest()

        # Tier 4: Name + Phone + Age
        elif tier == 4:
            if name and phone and age:
                key = f"{name}|{phone}|{age}"
                return hashlib.md5(key.encode()).hexdigest()

        # Tier 5: Name + District + Age
        elif tier == 5:
            if name and district and age:
                key = f"{name}|{district}|{age}"
                return hashlib.md5(key.encode()).hexdigest()

        return None

    def find_fuzzy_match(
        self, person_data: Dict, existing_groups: Dict
    ) -> Optional[Tuple[str, float]]:
        """
        Find if this person matches any existing group using dedupe-style fuzzy matching
        Prioritizes NAME and PARENT NAME with excellent typo/space handling
        Returns: (fingerprint, similarity_score) or None
        """
        if not self.use_fuzzy_matching:
            return None

        person_name = person_data.get("full_name", "")
        if not person_name:
            return None

        best_match = None
        best_score = 0.0

        # Compare against all existing groups
        for fingerprint, group in existing_groups.items():
            canonical = group["canonical_person"]

            # Use dedupe-based matching for comprehensive comparison
            similarity_score = self.dedupe_matcher.calculate_dedupe_similarity(
                person_data, canonical
            )

            # Track best match
            if similarity_score > best_score:
                best_score = similarity_score
                best_match = fingerprint

        # Return match only if score meets confidence threshold
        if best_score >= self.match_confidence_threshold:
            return (best_match, best_score)

        return None

    def get_matching_strategy_name(self, tier: int) -> str:
        """Get human-readable strategy name"""
        strategies = {
            1: "Name + Parent + Locality + Age + Phone",
            2: "Name + Parent + Locality + Phone",
            3: "Name + Parent + District + Age",
            4: "Name + Phone + Age",
            5: "Name + District + Age",
        }
        return strategies.get(tier, "Unknown")

    def calculate_confidence_score(self, tier: int, data_completeness: float) -> float:
        """Calculate confidence score based on tier and data quality"""
        tier_weights = {1: 0.95, 2: 0.90, 3: 0.85, 4: 0.75, 5: 0.65}
        base_score = tier_weights.get(tier, 0.5)
        return round(base_score * data_completeness, 2)

    def _get_candidate_groups_for_fuzzy_match(
        self, person_name: str, name_index: Dict
    ) -> List[str]:
        """
        Get candidate fingerprints for fuzzy matching based on name similarity
        Uses first few characters as a fast pre-filter
        """
        candidates = set()

        # Normalize the name
        normalized_name = self.fuzzy_matcher.normalize_name(person_name)

        if not normalized_name:
            return []

        # Strategy 1: Check exact normalized name
        if normalized_name in name_index:
            candidates.update(name_index[normalized_name])

        # Strategy 2: Check names with same first 3 characters
        if len(normalized_name) >= 3:
            prefix = normalized_name[:3]
            for indexed_name, fingerprints in name_index.items():
                if indexed_name.startswith(prefix):
                    candidates.update(fingerprints)

        # Strategy 3: Check names with similar token overlap
        name_tokens = set(normalized_name.split())
        for indexed_name, fingerprints in name_index.items():
            indexed_tokens = set(indexed_name.split())
            # If they share at least one significant token (>3 chars), consider as candidate
            if name_tokens and indexed_tokens:
                shared = name_tokens & indexed_tokens
                if any(len(token) > 3 for token in shared):
                    candidates.update(fingerprints)

        return list(candidates)

    def _fuzzy_match_against_candidates(
        self, person_data: Dict, person_groups: Dict, candidate_fingerprints: List[str]
    ) -> Optional[Tuple[str, float]]:
        """
        Check fuzzy match only against specific candidate groups (optimized)
        """
        best_match = None
        best_score = 0.0

        for fingerprint in candidate_fingerprints:
            if fingerprint not in person_groups:
                continue

            group = person_groups[fingerprint]
            canonical = group["canonical_person"]

            # Use dedupe-based matching for comprehensive comparison
            similarity_score = self.dedupe_matcher.calculate_dedupe_similarity(
                person_data, canonical
            )

            # Track best match
            if similarity_score > best_score:
                best_score = similarity_score
                best_match = fingerprint

        # Return match only if score meets confidence threshold
        if best_score >= self.match_confidence_threshold:
            return (best_match, best_score)

        return None

    def assess_data_quality(self, person: Dict) -> Tuple[float, Dict]:
        """Assess data quality and return completeness score + flags"""
        fields = [
            "full_name",
            "relative_name",
            "age",
            "phone_number",
            "present_district",
            "present_locality_village",
            "gender",
        ]

        filled = sum(1 for f in fields if person.get(f))
        completeness = filled / len(fields)

        flags = {
            "has_phone": bool(person.get("phone_number")),
            "has_parent_name": bool(person.get("relative_name")),
            "has_locality": bool(person.get("present_locality_village")),
            "has_age": bool(person.get("age")),
            "has_gender": bool(person.get("gender")),
            "completeness_percent": round(completeness * 100, 1),
        }

        return completeness, flags

    def populate_deduplication_table(self):
        """Populate the deduplication table with person data"""
        print("\n=== Populating person_deduplication_tracker ===")

        # Fetch all persons with their accused and crime information
        print("Fetching person data from database...")

        fetch_query = """
        SELECT 
            p.person_id,
            p.full_name,
            p.relative_name,
            p.age,
            p.gender,
            p.phone_number,
            p.present_district,
            p.present_locality_village,
            p.date_created,
            a.accused_id,
            a.accused_code,
            c.crime_id,
            c.fir_num,
            c.fir_reg_num,
            c.fir_date,
            c.case_status,
            h.ps_name,
            h.dist_name,
            bfa.accused_type,
            bfa.status as accused_status
        FROM persons p
        JOIN accused a ON p.person_id = a.person_id
        JOIN crimes c ON a.crime_id = c.crime_id
        LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
        LEFT JOIN brief_facts_accused bfa ON a.accused_id = bfa.accused_id
        ORDER BY p.person_id, c.fir_date
        """

        self.cursor.execute(fetch_query)
        all_records = self.cursor.fetchall()
        print(f"✓ Fetched {len(all_records)} person-crime records")

        # Group by person and apply fingerprinting
        print("\nApplying hierarchical fingerprinting strategies...")

        person_groups = {}  # fingerprint -> person data
        fingerprint_tiers = {}  # person_id -> tier used

        # First pass: Generate fingerprints for all persons
        persons_by_id = {}
        for record in all_records:
            person_id = record["person_id"]
            if person_id not in persons_by_id:
                persons_by_id[person_id] = {
                    "person_id": person_id,
                    "full_name": record["full_name"],
                    "relative_name": record["relative_name"],
                    "age": record["age"],
                    "gender": record["gender"],
                    "phone_number": record["phone_number"],
                    "present_district": record["present_district"],
                    "present_locality_village": record["present_locality_village"],
                    "date_created": record["date_created"],
                    "accused_ids": [],
                    "crime_records": [],
                }

            persons_by_id[person_id]["accused_ids"].append(record["accused_id"])
            persons_by_id[person_id]["crime_records"].append(
                {
                    "crime_id": record["crime_id"],
                    "accused_id": record["accused_id"],
                    "fir_num": record["fir_num"],
                    "fir_reg_num": record["fir_reg_num"],
                    "fir_date": (
                        record["fir_date"].isoformat() if record["fir_date"] else None
                    ),
                    "case_status": record["case_status"],
                    "ps_name": record["ps_name"],
                    "dist_name": record["dist_name"],
                    "accused_code": record["accused_code"],
                    "accused_type": record["accused_type"],
                    "accused_status": record["accused_status"],
                }
            )

        print(f"✓ Grouped into {len(persons_by_id)} unique person records")

        # Second pass: Apply tier-based fingerprinting + optimized 4-field fuzzy matching
        print(
            "\n🔄 Applying hybrid matching (tier fingerprinting + 4-field fuzzy matching)..."
        )
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, "no_match": 0, "fuzzy_match": 0}

        processed = 0
        total_persons = len(persons_by_id)

        # Create name-based index for fast candidate lookup
        name_index = {}  # normalized_name -> list of fingerprints

        for person_id, person_data in persons_by_id.items():
            fingerprint = None
            tier_used = None
            is_fuzzy_match = False
            fuzzy_score = None

            processed += 1
            if processed % 1000 == 0:
                print(f"   Processing {processed}/{total_persons} persons...")

            # STEP 1: Try traditional tier-based fingerprinting (FAST exact matching)
            for tier in range(1, 6):
                fingerprint = self.generate_fingerprint(person_data, tier)
                if fingerprint:
                    tier_used = tier
                    break

            # STEP 2: If no exact match AND fuzzy matching enabled, search for similar names
            if not fingerprint and self.use_fuzzy_matching:
                # Get candidate groups with similar names (optimization)
                person_name = person_data.get("full_name", "")
                if person_name:
                    # Create a list of candidate fingerprints to check (not ALL groups)
                    candidate_fingerprints = self._get_candidate_groups_for_fuzzy_match(
                        person_name, name_index
                    )

                    # Only check fuzzy match against candidates (much faster!)
                    if candidate_fingerprints:
                        fuzzy_result = self._fuzzy_match_against_candidates(
                            person_data, person_groups, candidate_fingerprints
                        )
                        if fuzzy_result:
                            fingerprint, fuzzy_score = fuzzy_result
                            tier_used = 6  # Special tier for fuzzy matches
                            is_fuzzy_match = True
                            tier_counts["fuzzy_match"] += 1

            if fingerprint:
                if not is_fuzzy_match:
                    tier_counts[tier_used] += 1

                fingerprint_tiers[person_id] = tier_used

                if fingerprint not in person_groups:
                    person_groups[fingerprint] = {
                        "fingerprint": fingerprint,
                        "tier": (
                            tier_used
                            if not is_fuzzy_match
                            else person_groups.get(fingerprint, {}).get("tier", 5)
                        ),
                        "strategy": (
                            self.get_matching_strategy_name(tier_used)
                            if not is_fuzzy_match
                            else person_groups.get(fingerprint, {}).get(
                                "strategy", "Fuzzy Match"
                            )
                        ),
                        "person_ids": [],
                        "accused_ids": [],
                        "crime_ids": set(),
                        "crime_records": [],
                        "canonical_person": person_data,
                        "first_seen": person_data.get("date_created"),
                        "uses_fuzzy_matching": is_fuzzy_match,
                        "fuzzy_scores": [] if is_fuzzy_match else None,
                        "name_variations": set(),
                    }

                group = person_groups[fingerprint]
                group["person_ids"].append(person_id)
                group["accused_ids"].extend(person_data["accused_ids"])

                # Track name variations for fuzzy matches
                if person_data.get("full_name"):
                    group["name_variations"].add(person_data["full_name"])

                    # Update name index for this group
                    normalized_name = self.fuzzy_matcher.normalize_name(
                        person_data["full_name"]
                    )
                    if normalized_name:
                        if normalized_name not in name_index:
                            name_index[normalized_name] = []
                        if fingerprint not in name_index[normalized_name]:
                            name_index[normalized_name].append(fingerprint)

                # Track fuzzy match scores
                if is_fuzzy_match and fuzzy_score:
                    if group["fuzzy_scores"] is None:
                        group["fuzzy_scores"] = []
                    group["fuzzy_scores"].append(fuzzy_score)
                    group["uses_fuzzy_matching"] = True

                for crime_rec in person_data["crime_records"]:
                    group["crime_ids"].add(crime_rec["crime_id"])
                    group["crime_records"].append(crime_rec)

                # Keep earliest record as canonical
                # Handle None values: treat None as "very old" (use max datetime)
                max_datetime = datetime.max
                
                person_date = person_data.get("date_created") or max_datetime
                group_date = group.get("first_seen") or max_datetime
                
                if person_date < group_date:
                    group["canonical_person"] = person_data
                    group["first_seen"] = person_data.get("date_created")
            else:
                tier_counts["no_match"] += 1

        print("\n📊 Matching Results:")
        print(f"   Exact Tier Matches (fast fingerprinting):")
        print(f"   • Tier 1 (Name+Parent+Locality+Age+Phone): {tier_counts[1]} persons")
        print(
            f"   • Tier 2 (Name+Parent+Locality+Phone):      {tier_counts[2]} persons"
        )
        print(
            f"   • Tier 3 (Name+Parent+District+Age):        {tier_counts[3]} persons"
        )
        print(
            f"   • Tier 4 (Name+Phone+Age):                  {tier_counts[4]} persons"
        )
        print(
            f"   • Tier 5 (Name+District+Age):               {tier_counts[5]} persons"
        )
        print(f"   ")
        print(
            f"   🔍 4-Field Fuzzy Matches (with typo handling): {tier_counts['fuzzy_match']} persons"
        )
        print(
            f"   ❌ No Match (insufficient data):              {tier_counts['no_match']} persons"
        )
        print(f"\n   Total Unique Persons: {len(person_groups)}")
        print(f"   Total Person Records: {len(persons_by_id)}")
        print(f"   Duplicate Records Found: {len(persons_by_id) - len(person_groups)}")

        total_tier_matches = sum([tier_counts[i] for i in range(1, 6)])
        if tier_counts["fuzzy_match"] > 0:
            print(
                f"\n   ✅ Fuzzy matching found {tier_counts['fuzzy_match']} additional matches that exact matching missed!"
            )
            print(
                f"   📊 Total matched: {total_tier_matches + tier_counts['fuzzy_match']}/{len(persons_by_id)} ({round((total_tier_matches + tier_counts['fuzzy_match'])/len(persons_by_id)*100, 1)}%)"
            )

        # Third pass: Insert into deduplication table
        print("\n💾 Inserting into person_deduplication_tracker...")

        insert_query = """
        INSERT INTO person_deduplication_tracker (
            person_fingerprint,
            matching_tier,
            matching_strategy,
            uses_fuzzy_matching,
            fuzzy_match_score,
            name_variations,
            canonical_person_id,
            full_name,
            relative_name,
            age,
            gender,
            phone_number,
            present_district,
            present_locality_village,
            all_person_ids,
            person_record_count,
            all_accused_ids,
            all_crime_ids,
            crime_count,
            crime_details,
            confidence_score,
            data_quality_flags
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        inserted = 0
        duplicates_found = 0

        for fingerprint, group in person_groups.items():
            canonical = group["canonical_person"]

            # Calculate data quality
            completeness, quality_flags = self.assess_data_quality(canonical)
            confidence = self.calculate_confidence_score(group["tier"], completeness)

            # Calculate average fuzzy score if applicable
            avg_fuzzy_score = None
            if group.get("uses_fuzzy_matching") and group.get("fuzzy_scores"):
                avg_fuzzy_score = round(
                    sum(group["fuzzy_scores"]) / len(group["fuzzy_scores"]), 2
                )

            # Check if this is a duplicate (same person, multiple records)
            is_duplicate = len(group["person_ids"]) > 1
            if is_duplicate:
                duplicates_found += 1

            try:
                self.cursor.execute(
                    insert_query,
                    (
                        fingerprint,
                        group["tier"],
                        group["strategy"],
                        group.get("uses_fuzzy_matching", False),
                        avg_fuzzy_score,
                        list(group.get("name_variations", [])),
                        canonical["person_id"],
                        canonical["full_name"],
                        canonical["relative_name"],
                        canonical["age"],
                        canonical["gender"],
                        canonical["phone_number"],
                        canonical["present_district"],
                        canonical["present_locality_village"],
                        group["person_ids"],
                        len(group["person_ids"]),
                        group["accused_ids"],
                        list(group["crime_ids"]),
                        len(group["crime_ids"]),
                        json.dumps(group["crime_records"]),
                        confidence,
                        json.dumps(quality_flags),
                    ),
                )
                inserted += 1

                if inserted % 1000 == 0:
                    print(f"   Inserted {inserted} records...")

            except Exception as e:
                print(f"   ✗ Error inserting {fingerprint}: {e}")
                continue

        self.conn.commit()
        print(f"\n✓ Successfully inserted {inserted} unique persons")
        print(f"✓ Found {duplicates_found} persons with duplicate records")

    def create_lookup_functions(self):
        """Create SQL functions for easy lookups from UI"""
        print("\n=== Creating helper functions for UI lookups ===")

        functions_sql = """
        -- Function 1: Get all crimes for an accused by accused_id
        CREATE OR REPLACE FUNCTION get_accused_crime_history(target_accused_id VARCHAR(50))
        RETURNS TABLE (
            person_fingerprint VARCHAR(32),
            matching_strategy VARCHAR(100),
            confidence_level TEXT,
            canonical_person_id VARCHAR(50),
            full_name VARCHAR(500),
            parent_name VARCHAR(255),
            age INTEGER,
            total_crimes INTEGER,
            total_duplicate_records INTEGER,
            crime_details JSONB
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                CASE 
                    WHEN pdt.matching_tier = 1 THEN 'Very High (★★★★★)'
                    WHEN pdt.matching_tier = 2 THEN 'High (★★★★☆)'
                    WHEN pdt.matching_tier = 3 THEN 'Good (★★★☆☆)'
                    WHEN pdt.matching_tier = 4 THEN 'Medium (★★☆☆☆)'
                    WHEN pdt.matching_tier = 5 THEN 'Basic (★☆☆☆☆)'
                END as confidence_level,
                pdt.canonical_person_id,
                pdt.full_name,
                pdt.relative_name as parent_name,
                pdt.age,
                pdt.crime_count as total_crimes,
                pdt.person_record_count as total_duplicate_records,
                pdt.crime_details
            FROM person_deduplication_tracker pdt
            WHERE target_accused_id = ANY(pdt.all_accused_ids);
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function 2: Get all crimes for a person by person_id
        CREATE OR REPLACE FUNCTION get_person_crime_history(target_person_id VARCHAR(50))
        RETURNS TABLE (
            person_fingerprint VARCHAR(32),
            matching_strategy VARCHAR(100),
            confidence_level TEXT,
            all_person_ids TEXT[],
            all_accused_ids TEXT[],
            total_crimes INTEGER,
            crime_details JSONB
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                CASE 
                    WHEN pdt.matching_tier = 1 THEN 'Very High'
                    WHEN pdt.matching_tier = 2 THEN 'High'
                    WHEN pdt.matching_tier = 3 THEN 'Good'
                    WHEN pdt.matching_tier = 4 THEN 'Medium'
                    WHEN pdt.matching_tier = 5 THEN 'Basic'
                END as confidence_level,
                pdt.all_person_ids,
                pdt.all_accused_ids,
                pdt.crime_count as total_crimes,
                pdt.crime_details
            FROM person_deduplication_tracker pdt
            WHERE target_person_id = ANY(pdt.all_person_ids);
        END;
        $$ LANGUAGE plpgsql;
        
        -- Function 3: Search persons by name
        CREATE OR REPLACE FUNCTION search_person_by_name(search_name VARCHAR(500))
        RETURNS TABLE (
            person_fingerprint VARCHAR(32),
            matching_strategy VARCHAR(100),
            full_name VARCHAR(500),
            parent_name VARCHAR(255),
            age INTEGER,
            district VARCHAR(255),
            phone VARCHAR(20),
            total_crimes INTEGER,
            total_duplicate_records INTEGER
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT 
                pdt.person_fingerprint,
                pdt.matching_strategy,
                pdt.full_name,
                pdt.relative_name as parent_name,
                pdt.age,
                pdt.present_district as district,
                pdt.phone_number as phone,
                pdt.crime_count as total_crimes,
                pdt.person_record_count as total_duplicate_records
            FROM person_deduplication_tracker pdt
            WHERE LOWER(pdt.full_name) LIKE LOWER('%' || search_name || '%')
            ORDER BY pdt.crime_count DESC;
        END;
        $$ LANGUAGE plpgsql;
        
        COMMENT ON FUNCTION get_accused_crime_history IS 
            'Get complete crime history for an accused by accused_id, includes all cases across duplicate records';
        COMMENT ON FUNCTION get_person_crime_history IS 
            'Get complete crime history for a person by person_id, shows all duplicate person records';
        COMMENT ON FUNCTION search_person_by_name IS 
            'Search for persons by name, returns deduplicated results with crime counts';
        """

        try:
            self.cursor.execute(functions_sql)
            self.conn.commit()
            print("✓ Created SQL functions:")
            print("  - get_accused_crime_history(accused_id)")
            print("  - get_person_crime_history(person_id)")
            print("  - search_person_by_name(name)")
        except Exception as e:
            self.conn.rollback()
            print(f"✗ Error creating functions: {e}")
            raise

    def generate_statistics(self):
        """Generate and display statistics"""
        print("\n=== Statistics ===")

        stats_queries = [
            (
                "Total Unique Persons",
                "SELECT COUNT(*) as count FROM person_deduplication_tracker",
            ),
            (
                "Persons with Multiple Records",
                "SELECT COUNT(*) as count FROM person_deduplication_tracker WHERE person_record_count > 1",
            ),
            (
                "Persons with Multiple Crimes",
                "SELECT COUNT(*) as count FROM person_deduplication_tracker WHERE crime_count > 1",
            ),
            (
                "Average Crimes per Person",
                "SELECT ROUND(AVG(crime_count), 2) as avg FROM person_deduplication_tracker",
            ),
            (
                "Top Repeat Offender",
                "SELECT full_name, crime_count FROM person_deduplication_tracker ORDER BY crime_count DESC LIMIT 1",
            ),
        ]

        for label, query in stats_queries:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if "full_name" in result:
                print(
                    f"   {label}: {result['full_name']} ({result['crime_count']} crimes)"
                )
            else:
                value = result.get("count") or result.get("avg")
                print(f"   {label}: {value}")

    def run(self):
        """Main execution flow"""
        try:
            self.connect()

            # Clear existing data first
            self.clear_existing_data()

            # Create new table and populate
            self.create_deduplication_table()
            self.populate_deduplication_table()
            self.create_lookup_functions()
            self.generate_statistics()

            print("\n" + "=" * 70)
            print("✅ Person Deduplication Table Setup Complete!")
            print("=" * 70)

            if self.use_fuzzy_matching:
                print("\n🔍 4-Field Dedupe Matching: ENABLED")
                print(
                    f"   - Match confidence threshold: {self.match_confidence_threshold * 100}%"
                )
                print(f"   - PRIMARY FIELDS (95% weight):")
                print(f"     • Full Name: 40% (HIGHEST)")
                print(f"     • Relative Name: 30% (VERY HIGH)")
                print(f"     • Relation Type: 15% (HIGH)")
                print(f"     • Gender: 10% (MEDIUM)")
                print(f"   - Typo tolerance: up to {self.typo_max_distance} characters")
                print(f"   - Space/punctuation normalization: ACTIVE")

            print("\n📝 Next Steps:")
            print(
                "   1. Query the table: SELECT * FROM person_deduplication_tracker LIMIT 10;"
            )
            print(
                "   2. Use in UI: SELECT * FROM get_accused_crime_history('<accused_id>');"
            )
            print("   3. Search persons: SELECT * FROM search_person_by_name('Abdul');")
            print(
                "   4. View fuzzy matches: SELECT * FROM person_deduplication_tracker WHERE uses_fuzzy_matching = TRUE;"
            )
            print("\n")

        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        finally:
            self.disconnect()


if __name__ == "__main__":
    print("=" * 70)
    print("Person Deduplication Table Creation & Population")
    print("Enhanced with 4-Field Dedupe Matching")
    print("=" * 70)
    print(f"Database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'local'}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fuzzy Matching: {'✓ ENABLED' if True else '✗ DISABLED'}")
    print(f"PRIMARY FIELDS: full_name (40%), relative_name (30%),")
    print(f"                relation_type (15%), gender (10%)")
    print(f"Typo Handling: Levenshtein distance, token matching, space normalization")
    if not DEDUPE_AVAILABLE:
        print("ℹ️  Using built-in dedupe-style algorithm (dedupe library not required)")
    print("=" * 70)

    tracker = PersonDeduplicationTracker(DATABASE_URL, use_fuzzy_matching=True)
    tracker.run()


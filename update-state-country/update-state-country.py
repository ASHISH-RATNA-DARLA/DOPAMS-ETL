#!/usr/bin/env python3
"""
Address State & Country Determination Script for DOPAMAS

This script:
1. Reads permanent_* address components (permanent_house_no, permanent_street_road_no, etc.) from persons table
2. Uses fast reference data lookup (ref.txt) for known states/countries
3. Falls back to LLM for unknown cases
4. Updates ONLY permanent_state_ut and permanent_country fields (never updates other permanent_* fields)

Logic:
- If both state and country already exist: Skip the record
- If state exists but country doesn't: Determine country only and update
- If state doesn't exist: Determine both state and country from permanent_* address fields
- If all permanent_* address fields are null: Set both state and country to null

Performance:
- Fast reference data lookup (ref.txt) for instant matching
- LLM used only when reference data doesn't have a match
- This makes processing much faster for known states/countries

The LLM has knowledge of:
- All Indian states, UTs, districts, and major localities
- All countries and their administrative divisions
- Common address patterns and landmarks
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import psycopg

# Ensure core is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm

from dotenv import load_dotenv

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# --- Configuration ---
load_dotenv()

DB_DSN = os.environ.get(
    "DB_DSN",
    " ".join([
        f"dbname={os.environ.get('DB_NAME', 'dopamas')}",
        f"user={os.environ.get('DB_USER')}",
        f"password={os.environ.get('DB_PASSWORD')}",
        f"host={os.environ.get('DB_HOST', 'localhost')}",
        f"port={os.environ.get('DB_PORT', '5432')}",
    ])
)

# LLM Configuration is now handled by core/llm_service.py
# Reference data file path
REF_DATA_FILE = os.environ.get("REF_DATA_FILE", "ref.txt")

# Table Configuration
DEFAULT_TABLE_NAME = os.environ.get("TABLE_NAME", "persons")
DEFAULT_ID_COLUMN = os.environ.get("ID_COLUMN", "id")

logger.info(f"Database: {os.environ.get('DB_NAME', 'dopamas')}@{os.environ.get('DB_HOST', 'localhost')}")
logger.info(f"Default Table: {DEFAULT_TABLE_NAME}, ID Column: {DEFAULT_ID_COLUMN}")
logger.info(f"Reference Data File: {REF_DATA_FILE}")


# --- Data Models ---

@dataclass
class AddressRecord:
    """Represents an address record from the database."""
    record_id: int  # Primary key (person_id)
    # Permanent address fields (input - read only, not updated)
    permanent_house_no: Optional[str]
    permanent_street_road_no: Optional[str]
    permanent_ward_colony: Optional[str]
    permanent_landmark_milestone: Optional[str]
    permanent_locality_village: Optional[str]
    permanent_area_mandal: Optional[str]
    permanent_district: Optional[str]
    # Permanent state and country (output - to be updated)
    permanent_state_ut: Optional[str]
    permanent_country: Optional[str]
    
    def get_address_components(self) -> str:
        """Get non-empty address components as formatted string from permanent_* fields."""
        components = []
        fields = [
            ("House No", self.permanent_house_no),
            ("Street/Road", self.permanent_street_road_no),
            ("Ward/Colony", self.permanent_ward_colony),
            ("Landmark", self.permanent_landmark_milestone),
            ("Locality/Village", self.permanent_locality_village),
            ("Area/Mandal", self.permanent_area_mandal),
            ("District", self.permanent_district),
        ]
        
        for label, value in fields:
            if value and str(value).strip():
                components.append(f"{label}: {value.strip()}")
        
        return ", ".join(components) if components else "No address components available"
    
    def needs_state_update(self) -> bool:
        """Check if state needs to be determined."""
        return not self.permanent_state_ut or str(self.permanent_state_ut).strip() == ""
    
    def needs_country_update(self) -> bool:
        """Check if country needs to be determined."""
        return not self.permanent_country or str(self.permanent_country).strip() == ""
    
    def has_both_state_and_country(self) -> bool:
        """Check if both state and country are already set."""
        state_ok = self.permanent_state_ut and str(self.permanent_state_ut).strip() != ""
        country_ok = self.permanent_country and str(self.permanent_country).strip() != ""
        return state_ok and country_ok
    
    def has_sufficient_info(self) -> bool:
        """Check if there's enough info to determine location from permanent_* fields."""
        key_fields = [
            self.permanent_locality_village,
            self.permanent_area_mandal,
            self.permanent_district,
        ]
        return any(field and str(field).strip() for field in key_fields)
    
    def has_any_address_info(self) -> bool:
        """Check if any permanent_* address field has a value."""
        address_fields = [
            self.permanent_house_no,
            self.permanent_street_road_no,
            self.permanent_ward_colony,
            self.permanent_landmark_milestone,
            self.permanent_locality_village,
            self.permanent_area_mandal,
            self.permanent_district,
        ]
        return any(field and str(field).strip() for field in address_fields)


@dataclass
class LocationResult:
    """Result of location determination."""
    state: Optional[str]
    country: Optional[str]
    confidence: str  # "high", "medium", "low"
    reasoning: str


# --- Reference Data Lookup ---

def parse_reference_data(file_path: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Parse ref.txt file to create a lookup dictionary.
    
    Returns: {
        "country_name": {
            "states": ["state1", "state2", ...],
            "cities": ["city1", "city2", ...]
        }
    }
    """
    ref_data = {}
    current_country = None
    
    if not os.path.exists(file_path):
        logger.warning(f"Reference data file not found: {file_path}. Will use LLM only.")
        return ref_data
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Check if line contains country marker
                if '(country):' in line:
                    country_name = line.split('(country):')[0].strip()
                    current_country = country_name
                    ref_data[current_country] = {"states": [], "cities": []}
                elif current_country:
                    # Check if it's a city (contains "city" or specific patterns)
                    if 'city' in line.lower() or 'village' in line.lower():
                        # Extract city name
                        city_name = line.split('(')[0].strip() if '(' in line else line
                        city_name = city_name.replace('city in', '').replace('city', '').replace('village', '').strip()
                        # Remove "in" and country name
                        city_name = city_name.replace(f'in {current_country}', '').strip()
                        if city_name:
                            ref_data[current_country]["cities"].append(city_name)
                    else:
                        # It's a state/UT
                        # Handle variations like "ANDHRA PRADESH (also listed as "Andhra Pradesh")"
                        state_name = line
                        
                        # Extract main state name (before parentheses)
                        if '(' in state_name:
                            state_name = state_name.split('(')[0].strip()
                        
                        # Remove common prefixes/suffixes
                        state_name = state_name.replace('also listed as', '').strip()
                        state_name = state_name.replace('listed twice', '').strip()
                        
                        # Remove quotes
                        state_name = state_name.strip('"').strip("'").strip()
                        
                        # Clean up
                        state_name = state_name.strip(')').strip('(').strip()
                        
                        if state_name and len(state_name) > 1:
                            # Add the state
                            ref_data[current_country]["states"].append(state_name)
                            
                            # Also add variations if mentioned (e.g., "odisha" vs "ODISHA")
                            # Extract variations from parentheses
                            if '(' in line and ')' in line:
                                variation = line.split('(')[1].split(')')[0]
                                variation = variation.replace('also listed as', '').strip()
                                variation = variation.strip('"').strip("'").strip()
                                if variation and variation.lower() != state_name.lower():
                                    ref_data[current_country]["states"].append(variation)
        
        logger.info(f"Loaded reference data: {len(ref_data)} countries")
        for country, data in ref_data.items():
            logger.info(f"  {country}: {len(data['states'])} states, {len(data['cities'])} cities")
        
    except Exception as e:
        logger.error(f"Error parsing reference data file: {e}", exc_info=True)
        return {}
    
    return ref_data


def normalize_text(text: str) -> str:
    """Normalize text for matching (lowercase, remove extra spaces)."""
    if not text:
        return ""
    return " ".join(text.lower().split())


def lookup_state_country(
    state_name: Optional[str],
    address_components: str,
    ref_data: Dict[str, Dict[str, List[str]]]
) -> Optional[Tuple[str, str]]:
    """
    Fast lookup using reference data.
    
    Returns: (state, country) tuple if found, None otherwise.
    """
    if not ref_data:
        return None
    
    # If state is provided, try to match it
    if state_name:
        normalized_state = normalize_text(state_name)
        
        for country, data in ref_data.items():
            # Check states - exact match
            for ref_state in data["states"]:
                normalized_ref = normalize_text(ref_state)
                if normalized_ref == normalized_state:
                    return (ref_state, country)  # Return the reference state name (standardized)
            
            # Check states - partial match (e.g., "TELANGANA" matches "Telangana")
            for ref_state in data["states"]:
                normalized_ref = normalize_text(ref_state)
                # Check if one contains the other (for variations)
                if normalized_ref in normalized_state or normalized_state in normalized_ref:
                    if len(normalized_ref) > 3 and len(normalized_state) > 3:  # Avoid short false matches
                        return (ref_state, country)
            
            # Check cities (if state matches a city, return the country)
            for ref_city in data["cities"]:
                normalized_ref = normalize_text(ref_city)
                if normalized_ref == normalized_state or normalized_ref in normalized_state:
                    return (state_name, country)  # Keep original state name if it's a city
    
    # Try to find state in address components
    address_lower = normalize_text(address_components)
    
    for country, data in ref_data.items():
        # Check states in address - look for exact matches first
        for ref_state in data["states"]:
            normalized_ref = normalize_text(ref_state)
            # Check if state name appears in address
            if normalized_ref in address_lower:
                # Make sure it's not part of a longer word
                words = address_lower.split()
                if normalized_ref in words or any(normalized_ref in word for word in words if len(word) >= len(normalized_ref)):
                    return (ref_state, country)
        
        # Check cities in address
        for ref_city in data["cities"]:
            normalized_ref = normalize_text(ref_city)
            if normalized_ref in address_lower:
                # For cities, we might not know the state, but we know the country
                return (None, country)
    
    return None


# --- LLM Client ---

class LocationDeterminationLLM:
    """LLM client for determining state and country from address."""
    
    def __init__(self, ref_data: Dict[str, Dict[str, List[str]]] = None):
        self.llm_service = get_llm('classification')
        self.ref_data = ref_data or {}
    
    def determine_location(self, address: AddressRecord) -> LocationResult:
        """
        Determine state and country from address components.
        First tries fast reference data lookup, then falls back to LLM if needed.
        
        Returns LocationResult with state, country, confidence, and reasoning.
        """
        components = address.get_address_components()
        
        # Try fast reference data lookup first
        if self.ref_data:
            lookup_result = lookup_state_country(
                state_name=address.permanent_state_ut,
                address_components=components,
                ref_data=self.ref_data
            )
            
            if lookup_result:
                ref_state, ref_country = lookup_result
                logger.debug(f"Found match in reference data: state={ref_state}, country={ref_country}")
                
                # If state already exists, we only need country
                if address.permanent_state_ut and str(address.permanent_state_ut).strip():
                    return LocationResult(
                        state=address.permanent_state_ut,  # Keep existing state
                        country=ref_country,
                        confidence="high",
                        reasoning=f"Matched country '{ref_country}' from reference data for state '{address.permanent_state_ut}'"
                    )
                else:
                    # Both state and country from reference
                    return LocationResult(
                        state=ref_state,
                        country=ref_country,
                        confidence="high",
                        reasoning=f"Matched state '{ref_state}' and country '{ref_country}' from reference data"
                    )
        
        # If reference lookup didn't find a match, use LLM
        logger.debug("No match in reference data, using LLM")
        prompt = self._build_prompt(address)
        
        try:
            result = self._call_llm(prompt)
            return result
        except Exception as e:
            logger.error(f"LLM failed: {e}")
            return LocationResult(
                state=None,
                country=None,
                confidence="low",
                reasoning=f"LLM failed: {str(e)}"
            )
    
    def _build_prompt(self, address: AddressRecord) -> str:
        """Build intelligent prompt based on what information is available."""
        
        components = address.get_address_components()
        
        # Scenario 1: No state, need both state and country
        if address.needs_state_update():
            prompt = f"""You are an expert in Indian and international geography with comprehensive knowledge of:
- All Indian states, union territories, districts, taluks, mandals, and major localities
- All countries and their administrative divisions worldwide
- Common address patterns, landmarks, and naming conventions

Task: Determine the STATE/UT and COUNTRY from the following address components:

{components}

Instructions:
1. Analyze ALL available address components carefully
2. Use district/locality/area names to identify the state/UT
3. If district/locality clearly indicates India, identify the specific state/UT
4. Determine the country (India or other country)
5. Provide confidence level: "high" (certain), "medium" (probable), or "low" (uncertain)

Return ONLY a JSON object in this exact format:
{{
  "state": "Full State/UT Name or null if not determinable",
  "country": "Full Country Name",
  "confidence": "high/medium/low",
  "reasoning": "Brief explanation of how you determined the location"
}}

Examples:
- District: "Hyderabad" → {{"state": "Telangana", "country": "India", "confidence": "high"}}
- District: "Mumbai" → {{"state": "Maharashtra", "country": "India", "confidence": "high"}}
- District: "Pune" → {{"state": "Maharashtra", "country": "India", "confidence": "high"}}
- District: "Bangalore Urban" → {{"state": "Karnataka", "country": "India", "confidence": "high"}}
- Locality: "Connaught Place", District: "New Delhi" → {{"state": "Delhi", "country": "India", "confidence": "high"}}
- District: "Kathmandu" → {{"state": null, "country": "Nepal", "confidence": "high"}}

Return ONLY the JSON object, no other text."""
        
        # Scenario 2: State available, need only country
        else:
            prompt = f"""You are an expert in world geography with knowledge of all countries and their states/provinces.

Task: Determine the COUNTRY from the following address:

{components}

The address already has State/UT: {address.permanent_state_ut}

Instructions:
1. Use the state/UT name and other address components to determine the country
2. Most Indian states/UTs are unambiguous (e.g., "Telangana" → India)
3. Some state names may exist in multiple countries (verify using other components)
4. Provide confidence level based on certainty

Return ONLY a JSON object:
{{
  "state": "{address.permanent_state_ut}",
  "country": "Full Country Name",
  "confidence": "high/medium/low",
  "reasoning": "Brief explanation"
}}

Common Indian States/UTs: Andhra Pradesh, Telangana, Karnataka, Maharashtra, Tamil Nadu, Kerala, Delhi, Punjab, Haryana, Uttar Pradesh, Madhya Pradesh, Rajasthan, Gujarat, West Bengal, Odisha, Bihar, Jharkhand, Chhattisgarh, Assam, etc.

Return ONLY the JSON object."""
        
        return prompt
    
    def _call_llm(self, prompt: str) -> LocationResult:
        """Call LLM with the prompt and parse response."""
        
        logger.debug(f"Calling LLM: {self.llm_service.model}")
        start_time = time.time()
        
        response_text = self.llm_service.generate(prompt=prompt)
        
        elapsed = time.time() - start_time
        
        if not response_text:
            raise Exception("Empty response from LLM")
            
        logger.debug(f"LLM response received in {elapsed:.2f}s")
        
        # Parse JSON from response
        location_data = self._extract_json(response_text)
        
        return LocationResult(
            state=location_data.get("state"),
            country=location_data.get("country"),
            confidence=location_data.get("confidence", "low"),
            reasoning=location_data.get("reasoning", "No reasoning provided")
        )
    
    def _extract_json(self, text: str) -> Dict[str, Any]:
        """Extract JSON from LLM response."""
        text = text.strip()
        
        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        
        # Find JSON object
        start = text.find("{")
        end = text.rfind("}")
        
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        # If parsing fails, return empty
        logger.warning(f"Could not parse JSON from: {text[:200]}")
        return {
            "state": None,
            "country": None,
            "confidence": "low",
            "reasoning": "Failed to parse LLM response"
        }


# --- Database Operations ---

def fetch_records_needing_update(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
    update_state: bool = True,
    update_country: bool = True
) -> List[AddressRecord]:
    """
    Fetch records that need state/country updates.
    
    Reads from permanent_* address fields and checks if permanent_state_ut or permanent_country need updates.
    Skips records where both state and country are already set.
    
    Args:
        table_name: Name of the table (e.g., 'persons')
        id_column: Primary key column name (e.g., 'person_id' or 'id')
        limit: Maximum number of records to fetch (None = process all records)
        update_state: Include records needing state updates
        update_country: Include records needing country updates
    """
    conditions = []
    
    if update_state:
        conditions.append("(permanent_state_ut IS NULL OR permanent_state_ut = '')")
    
    if update_country:
        conditions.append("(permanent_country IS NULL OR permanent_country = '')")
    
    if not conditions:
        logger.warning("No update conditions specified")
        return []
    
    where_clause = " OR ".join(conditions)
    
    # Exclude records where both state and country are already set (skip those)
    where_clause = f"({where_clause}) AND NOT (permanent_state_ut IS NOT NULL AND permanent_state_ut != '' AND permanent_country IS NOT NULL AND permanent_country != '')"
    
    # Build query with optional LIMIT
    query = f"""
        SELECT 
            {id_column},
            permanent_house_no,
            permanent_street_road_no,
            permanent_ward_colony,
            permanent_landmark_milestone,
            permanent_locality_village,
            permanent_area_mandal,
            permanent_district,
            permanent_state_ut,
            permanent_country
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY {id_column}
    """
    
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)
        logger.info(f"Fetching up to {limit} records from {table_name} where {where_clause}")
    else:
        params = ()
        logger.info(f"Fetching ALL records from {table_name} where {where_clause}")
    
    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    
    records = []
    for row in rows:
        record = AddressRecord(
            record_id=row[0],
            permanent_house_no=row[1],
            permanent_street_road_no=row[2],
            permanent_ward_colony=row[3],
            permanent_landmark_milestone=row[4],
            permanent_locality_village=row[5],
            permanent_area_mandal=row[6],
            permanent_district=row[7],
            permanent_state_ut=row[8],
            permanent_country=row[9],
        )
        records.append(record)
    
    logger.info(f"Fetched {len(records)} records needing updates")
    return records


def update_location(
    table_name: str,
    id_column: str,
    record_id: int,
    state: Optional[str],
    country: Optional[str],
    update_state_field: bool = True,
    update_country_field: bool = True
) -> None:
    """Update state and country in the database. Can set to NULL if None is passed."""
    
    updates = []
    params = []
    
    # Update state if requested
    if update_state_field:
        if state is not None:
            # If state is empty string, convert to None (NULL)
            state_value = state.strip() if state and state.strip() else None
            updates.append("permanent_state_ut = %s")
            params.append(state_value)
        else:
            # Explicitly set to NULL
            updates.append("permanent_state_ut = NULL")
    
    # Update country if requested
    if update_country_field:
        if country is not None:
            # If country is empty string, convert to None (NULL)
            country_value = country.strip() if country and country.strip() else None
            updates.append("permanent_country = %s")
            params.append(country_value)
        else:
            # Explicitly set to NULL
            updates.append("permanent_country = NULL")
    
    if not updates:
        logger.warning(f"No updates to perform for record {record_id}")
        return
    
    params.append(record_id)
    
    query = f"""
        UPDATE {table_name}
        SET {', '.join(updates)}
        WHERE {id_column} = %s
    """
    
    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    
    logger.info(f"Updated {table_name}.{id_column}={record_id}: state={state}, country={country}")


# --- Main Pipeline ---

def process_records(
    table_name: str,
    id_column: str,
    limit: Optional[int] = None,
    dry_run: bool = False,
    update_state: bool = True,
    update_country: bool = True
) -> None:
    """
    Main processing pipeline.
    
    Processes all records one by one by person_id from the persons table.
    
    Args:
        table_name: Database table name
        id_column: Primary key column name
        limit: Max records to process (None = process all records)
        dry_run: If True, don't update database
        update_state: Whether to update state field
        update_country: Whether to update country field
    """
    logger.info("="*80)
    logger.info("Address State & Country Determination Pipeline")
    logger.info("="*80)
    logger.info(f"Table: {table_name}")
    logger.info(f"ID Column: {id_column}")
    logger.info(f"Limit: {'ALL records' if limit is None else limit}")
    logger.info(f"Dry Run: {dry_run}")
    logger.info(f"Update State: {update_state}")
    logger.info(f"Update Country: {update_country}")
    logger.info("="*80)
    
    # Load reference data for fast lookup
    logger.info("Loading reference data...")
    ref_data = parse_reference_data(REF_DATA_FILE)
    if ref_data:
        logger.info(f"✓ Reference data loaded: {len(ref_data)} countries")
    else:
        logger.info("⚠ No reference data available, will use LLM only")
    
    # Initialize LLM client with reference data
    llm = LocationDeterminationLLM(
        ref_data=ref_data
    )
    
    # Fetch records
    records = fetch_records_needing_update(
        table_name=table_name,
        id_column=id_column,
        limit=limit,
        update_state=update_state,
        update_country=update_country
    )
    
    if not records:
        logger.info("No records found needing updates")
        return
    
    # Process each record
    total = len(records)
    successful = 0
    failed = 0
    skipped = 0
    
    for idx, record in enumerate(records, 1):
        logger.info("-"*80)
        logger.info(f"Processing {idx}/{total}: ID={record.record_id}")
        logger.info(f"Current: state={record.permanent_state_ut}, country={record.permanent_country}")
        logger.info(f"Address: {record.get_address_components()}")
        
        # Skip if both state and country are already set
        if record.has_both_state_and_country():
            logger.info(f"Skipping ID={record.record_id}: Both state and country already set")
            skipped += 1
            continue
        
        try:
            # Rule 4: If all permanent_* address fields are null, set both to null
            if not record.has_any_address_info():
                logger.info(f"No address information available for ID={record.record_id}, setting state and country to null")
                state_to_update = None
                country_to_update = None
            else:
                # Rule 2: If state exists, determine country only
                if record.permanent_state_ut and str(record.permanent_state_ut).strip():
                    logger.info(f"State already exists ({record.permanent_state_ut}), determining country only")
                    # Determine location using LLM (will only determine country)
                    start_time = time.time()
                    result = llm.determine_location(record)
                    elapsed = time.time() - start_time
                    
                    logger.info(f"LLM Result ({elapsed:.2f}s):")
                    logger.info(f"  State: {result.state} (keeping existing: {record.permanent_state_ut})")
                    logger.info(f"  Country: {result.country}")
                    logger.info(f"  Confidence: {result.confidence}")
                    logger.info(f"  Reasoning: {result.reasoning}")
                    
                    state_to_update = None  # Don't update state, it already exists
                    country_to_update = result.country if result.country else None
                
                # Rule 3: If state doesn't exist, determine both from permanent_* fields
                else:
                    logger.info(f"State not available, determining both state and country from address")
                    # Determine location using LLM (will determine both)
                    start_time = time.time()
                    result = llm.determine_location(record)
                    elapsed = time.time() - start_time
                    
                    logger.info(f"LLM Result ({elapsed:.2f}s):")
                    logger.info(f"  State: {result.state}")
                    logger.info(f"  Country: {result.country}")
                    logger.info(f"  Confidence: {result.confidence}")
                    logger.info(f"  Reasoning: {result.reasoning}")
                    
                    state_to_update = result.state if result.state else None
                    country_to_update = result.country if result.country else None
            
            # Update database
            if not dry_run:
                # Determine which fields to update
                # Rule 2: If state exists, only update country (state_to_update is None means don't update state)
                # Rule 3: If state doesn't exist, update both
                # Rule 4: If no address info, set both to null (state_to_update and country_to_update are both None)
                
                update_state_field = (state_to_update is not None) or (not record.has_any_address_info() and record.needs_state_update())
                update_country_field = (country_to_update is not None) or (not record.has_any_address_info() and record.needs_country_update())
                
                # Special case: If state exists, we don't update it even if state_to_update is None
                if record.permanent_state_ut and str(record.permanent_state_ut).strip():
                    update_state_field = False
                
                if update_state_field or update_country_field:
                    update_location(
                        table_name=table_name,
                        id_column=id_column,
                        record_id=record.record_id,
                        state=state_to_update if update_state_field else None,
                        country=country_to_update if update_country_field else None,
                        update_state_field=update_state_field,
                        update_country_field=update_country_field
                    )
                    successful += 1
                    logger.info(f"✓ Successfully updated ID={record.record_id}")
                else:
                    logger.warning(f"No updates needed for ID={record.record_id}")
                    skipped += 1
            else:
                logger.info(f"[DRY RUN] Would update: state={state_to_update}, country={country_to_update}")
                successful += 1
        
        except Exception as e:
            logger.error(f"Failed to process ID={record.record_id}: {e}", exc_info=True)
            failed += 1
            continue
    
    # Summary
    logger.info("="*80)
    logger.info("Pipeline Summary:")
    logger.info(f"  Total records: {total}")
    logger.info(f"  Successful: {successful}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Skipped: {skipped}")
    logger.info("="*80)


# --- CLI ---

def main():
    parser = argparse.ArgumentParser(
        description="Determine and update state/country from address components using LLM"
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_NAME,
        help=f"Table name (default: {DEFAULT_TABLE_NAME} from .env or 'persons')"
    )
    parser.add_argument(
        "--id-column",
        default=DEFAULT_ID_COLUMN,
        help=f"Primary key column name (default: {DEFAULT_ID_COLUMN} from .env or 'id')"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum records to process (default: process ALL records). Use --limit N to limit to N records."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't update database, just show what would be updated"
    )
    parser.add_argument(
        "--update-state",
        action="store_true",
        default=True,
        help="Update state/UT field (default: True)"
    )
    parser.add_argument(
        "--update-country",
        action="store_true",
        default=True,
        help="Update country field (default: True)"
    )
    parser.add_argument(
        "--skip-state",
        action="store_true",
        help="Don't update state field"
    )
    parser.add_argument(
        "--skip-country",
        action="store_true",
        help="Don't update country field"
    )
    
    args = parser.parse_args()
    
    # Handle skip flags
    update_state = not args.skip_state
    update_country = not args.skip_country
    
    process_records(
        table_name=args.table,
        id_column=args.id_column,
        limit=args.limit,
        dry_run=args.dry_run,
        update_state=update_state,
        update_country=update_country
    )


if __name__ == "__main__":
    main()

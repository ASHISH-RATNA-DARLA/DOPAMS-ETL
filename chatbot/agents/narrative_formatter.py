"""
Agent 4: Narrative Formatter
Converts raw query results into natural language narratives using LLM
"""

import logging
import json
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class NarrativeFormatterAgent:
    """
    Agent 4: Transforms raw database results into natural language narratives
    
    Uses LLM to create professional, easy-to-understand responses from query results.
    Includes Redis caching for fast repeated queries.
    """
    
    def __init__(self, llm_client, cache_manager=None):
        """
        Initialize narrative formatter agent
        
        Args:
            llm_client: LLM client for generating narratives
            cache_manager: Redis cache manager for caching narratives
        """
        self.llm = llm_client
        self.cache = cache_manager
        self.enabled = True  # Can be controlled via config
        
    def format_results(
        self,
        user_question: str,
        query_results: Dict[str, List[Dict]],
        query_metadata: Optional[Dict] = None,
        format_preferences: Optional[Dict] = None
    ) -> str:
        """
        Convert raw query results into natural language narrative
        RESPECTS user's format preferences (brief/detailed, casual/professional, list/narrative)
        
        Args:
            user_question: Original user question
            query_results: Dict with 'postgresql' and/or 'mongodb' results
            query_metadata: Optional metadata (query type, entities detected, etc.)
            format_preferences: User's format preferences (from conversation patterns)
            
        Returns:
            Natural language narrative string
        """
        # If narrative formatting disabled, return fallback
        if not self.enabled:
            return self._fallback_formatting(user_question, query_results)
        
        # Check cache first (using Redis)
        cache_key = self._generate_cache_key(user_question, query_results)
        if self.cache:
            cached_narrative = self.cache.get_value(f"narrative:{cache_key}")
            if cached_narrative:
                logger.info("Narrative retrieved from cache")
                return cached_narrative
        
        # Generate narrative using LLM (with format preferences)
        narrative = self._generate_narrative(user_question, query_results, query_metadata, format_preferences)
        
        # Cache the narrative (TTL from Config)
        if self.cache and narrative:
            from config import Config
            ttl = Config.NARRATIVE_CACHE_TTL
            self.cache.set_value(f"narrative:{cache_key}", narrative, ttl=ttl)
            logger.info(f"Narrative cached for future use (TTL: {ttl}s)")
        
        return narrative
    
    def _generate_narrative(
        self,
        user_question: str,
        query_results: Dict[str, List[Dict]],
        query_metadata: Optional[Dict],
        format_preferences: Optional[Dict] = None
    ) -> str:
        """Generate narrative using LLM with format preferences"""
        
        # Prepare data summary
        v2_data = query_results.get('postgresql', [])
        v1_data = query_results.get('mongodb', [])
        
        total_results = len(v2_data) + len(v1_data)
        
        # If no results, return friendly message
        if total_results == 0:
            return "I didn't find any matching records for your query. Try adjusting your search criteria or ask for help to see what information is available."
        
        # Extract format preferences
        prefs = format_preferences or {}
        length = prefs.get('length', 'normal')
        style = prefs.get('style', 'professional')
        format_type = prefs.get('format', 'narrative')
        
        # Build dynamic system prompt based on preferences
        system_prompt = """You are a professional crime investigation analyst assistant.
Convert raw database query results into clear, professional narratives.

CRITICAL TERMINOLOGY:
- ALWAYS refer to PostgreSQL data as "V2 data" (NEVER just "PostgreSQL data")
- ALWAYS refer to MongoDB data as "V1 data" (NEVER just "MongoDB data")
- Example: "From V2 data (PostgreSQL), I found..." ✅
- Example: "From PostgreSQL data..." ❌ WRONG!

RULES:
1. Start with a brief summary (1-2 sentences)
2. Present key information in natural language
3. Use {tone} tone
4. Include important numbers and statistics
5. Use proper formatting (paragraphs, bullets if helpful)
6. Don't invent information not in the data
7. ALWAYS use "V1 data" and "V2 data" terminology when mentioning data sources
8. For crime data, include: FIR number, type, status, location, accused count
9. For person data, include: name, age, occupation, location
10. For statistics, highlight key insights

CRITICAL - DO NOT INTERPRET OR ASSUME:
- For classification/category queries (e.g., "crimes by class_classification"):
  → ONLY report the actual values from the database
  → DO NOT add interpretations like "indicates complex activities"
  → DO NOT explain what classifications might mean
  → DO NOT make assumptions about severity, nature, or examples
  → Just state: "X classification has Y crimes" ✅
  → Example: "Intermediate: 12,280 crimes" (GOOD!)
  → Example: "Intermediate suggests complex illicit activities" (BAD! Don't do this!)

{length_instruction}

{format_instruction}

REMEMBER: V2 = PostgreSQL, V1 = MongoDB. Use these terms consistently!
REMEMBER: Report facts from data, don't interpret or assume meanings!""".format(
            tone='professional and friendly' if style == 'professional' else 'casual and friendly',
            length_instruction={
                'brief': 'Keep response VERY brief (1-2 sentences max).',
                'normal': 'Keep response concise but informative (2-4 paragraphs max).',
                'detailed': 'Provide comprehensive details (3-6 paragraphs, include all important fields).'
            }.get(length, 'Keep response concise but informative (2-4 paragraphs max).'),
            format_instruction={
                'list': 'Format response as a bulleted list with key points.',
                'table': 'Present information in a structured, organized way.',
                'narrative': 'Use flowing narrative prose (paragraphs).'
            }.get(format_type, 'Use flowing narrative prose (paragraphs).')
        )
        
        # Prepare data for LLM (limit size to avoid token overflow)
        # Pass user_question so we can prioritize requested fields
        data_summary = self._prepare_data_summary(v2_data, v1_data, total_results, user_question)
        
        # Check if user asked for specific fields (comprehensive detection)
        question_lower = user_question.lower()
        emphasize_nationality = 'nationality' in question_lower or 'domicile' in question_lower or 'native' in question_lower or 'interstate' in question_lower or 'international' in question_lower
        emphasize_transport = 'transport' in question_lower or 'transport method' in question_lower
        emphasize_supply_chain = 'supply chain' in question_lower or 'supply' in question_lower
        emphasize_packaging = 'packaging' in question_lower or 'package' in question_lower
        emphasize_weight = 'weight' in question_lower or 'quantity' in question_lower
        emphasize_seizure = 'seizure' in question_lower
        emphasize_commercial = 'commercial' in question_lower
        emphasize_purity = 'purity' in question_lower
        emphasize_value = 'street value' in question_lower or ('value' in question_lower and 'street' in question_lower)
        
        # Add emphasis instructions if user asked for specific fields
        emphasis_instruction = ""
        if emphasize_nationality:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about NATIONALITY/DOMICILE CLASSIFICATION!
- Check the data for 'domicile_classification' field - it WILL be included even if NULL
- If domicile_classification is NULL or empty, explicitly state: "The domicile_classification field exists in the database but is NULL/empty for this record"
- If domicile_classification has a value, prominently highlight it and EXPLAIN what it means:
  * "native state" or "native" = Person belongs to Telangana state (India)
  * "inter state" or "interstate" = Person belongs to India but NOT Telangana (other Indian states)
  * "international" = Person belongs to outside India (foreign country)
- Put nationality/domicile classification information FIRST in your response!
- Always explain what the value means (native = Telangana, inter state = other Indian states, international = outside India)
- DO NOT say "not explicitly mentioned" if the field exists but is NULL - say "is NULL" or "not available" instead"""
        elif emphasize_packaging:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about PACKAGING DETAILS!
- Check the data for 'packaging_details' field - it WILL be included even if NULL
- If packaging_details is NULL or empty, explicitly state: "The packaging_details field exists in the database but is NULL/empty for this record"
- If packaging_details has a value, prominently highlight it and describe it in detail
- Also check number_of_packets and weight_breakdown fields (related to packaging)
- Put packaging information FIRST in your response!
- DO NOT say "not explicitly mentioned" if the field exists but is NULL - say "is NULL" or "not available" instead"""
        elif emphasize_transport:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about TRANSPORT METHOD!
- Check the data for 'transport_method' field - it WILL be included even if NULL
- If transport_method is NULL or empty, explicitly state: "The transport_method field exists in the database but is NULL/empty for this record"
- If transport_method has a value, prominently highlight it
- Also check source_location and destination fields
- Put transport method information FIRST in your response!
- DO NOT say "not explicitly mentioned" if the field exists but is NULL - say "is NULL" or "not available" instead"""
        elif emphasize_supply_chain:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about SUPPLY CHAIN!
- Check the data for 'supply_chain' field - it WILL be included even if NULL
- If supply_chain is NULL or empty, explicitly state: "The supply_chain field exists in the database but is NULL/empty for this record"
- Also check source_location, destination, and transport_method fields
- Put supply chain information FIRST in your response!
- DO NOT say "not explicitly mentioned" if the field exists but is NULL - say "is NULL" or "not available" instead"""
        elif emphasize_weight:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about WEIGHT/QUANTITY!
- Check the data for weight_breakdown, total_quantity, quantity_numeric, quantity_unit, number_of_packets fields
- These fields WILL be included even if NULL
- If any are NULL, explicitly state which fields are NULL
- Put weight/quantity information FIRST in your response!"""
        elif emphasize_seizure:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about SEIZURE!
- Check the data for seizure_location, seizure_time, seizure_method, seizure_officer, seizure_worth fields
- These fields WILL be included even if NULL
- Put seizure information FIRST in your response!"""
        elif emphasize_commercial:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about COMMERCIAL QUANTITY!
- Check the data for is_commercial (boolean), commercial_quantity, total_quantity, quantity_numeric fields
- These fields WILL be included even if NULL
- Put commercial quantity information FIRST in your response!"""
        elif emphasize_purity:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about PURITY!
- Check the data for purity, drug_name, scientific_name fields
- These fields WILL be included even if NULL
- Put purity information FIRST in your response!"""
        elif emphasize_value:
            emphasis_instruction = """\n\n⚠️ CRITICAL: User specifically asked about STREET VALUE!
- Check the data for street_value, street_value_numeric, seizure_worth fields
- These fields WILL be included even if NULL
- Put value information FIRST in your response!"""
        
        user_prompt = f"""User asked: "{user_question}"

Query returned: {total_results} result(s)

Data:
{data_summary}
{emphasis_instruction}

Convert this into a professional, easy-to-understand narrative response."""

        try:
            # Generate narrative using LLM
            # Note: max_tokens and temperature are configured in LLMConfig, not here
            narrative = self.llm.generate(
                prompt=user_prompt,
                system_prompt=system_prompt
            )
            
            if narrative:
                logger.info(f"Generated narrative ({len(narrative)} chars)")
                return narrative.strip()
            else:
                logger.warning("LLM returned empty narrative, using fallback")
                return self._fallback_formatting(user_question, query_results)
                
        except Exception as e:
            logger.error(f"Narrative generation error: {e}")
            return self._fallback_formatting(user_question, query_results)
    
    def _prepare_data_summary(
        self,
        v2_data: List[Dict],
        v1_data: List[Dict],
        total_results: int,
        user_question: str = ""
    ) -> str:
        """Prepare concise data summary for LLM (avoid token overflow)
        
        Always includes fields that user specifically asked for, even if NULL.
        """
        
        # Detect what fields user asked for - comprehensive drug field detection
        question_lower = user_question.lower()
        requested_fields = []
        
        # Nationality/Domicile related
        if 'nationality' in question_lower or 'domicile' in question_lower or 'native' in question_lower or 'interstate' in question_lower or 'international' in question_lower:
            requested_fields.extend(['domicile_classification'])
        
        # Transport method related
        if 'transport' in question_lower or 'transport method' in question_lower:
            requested_fields.extend(['transport_method', 'source_location', 'destination'])
        
        # Supply chain related
        if 'supply chain' in question_lower or 'supply' in question_lower:
            requested_fields.extend(['supply_chain', 'source_location', 'destination', 'transport_method'])
        
        # Packaging related
        if 'packaging' in question_lower or 'package' in question_lower:
            requested_fields.extend(['packaging_details', 'number_of_packets', 'weight_breakdown'])
        
        # Weight/quantity related
        if 'weight' in question_lower or 'quantity' in question_lower:
            requested_fields.extend(['weight_breakdown', 'total_quantity', 'quantity_numeric', 'quantity_unit', 'number_of_packets'])
        
        # Seizure related
        if 'seizure' in question_lower:
            requested_fields.extend(['seizure_location', 'seizure_time', 'seizure_method', 'seizure_officer', 'seizure_worth'])
        
        # Commercial quantity related
        if 'commercial' in question_lower:
            requested_fields.extend(['is_commercial', 'commercial_quantity', 'total_quantity', 'quantity_numeric'])
        
        # Purity related
        if 'purity' in question_lower:
            requested_fields.extend(['purity', 'drug_name', 'scientific_name'])
        
        # Street value related
        if 'street value' in question_lower or 'value' in question_lower:
            requested_fields.extend(['street_value', 'street_value_numeric', 'seizure_worth'])
        
        summary_parts = []
        
        # V2 Data (PostgreSQL) - ALWAYS use V2 label!
        if v2_data:
            summary_parts.append(f"**V2 Data (PostgreSQL):** {len(v2_data)} records")
            
            # Show first few records with key fields
            preview = v2_data[:3]  # Limit to first 3 for token efficiency
            for i, record in enumerate(preview, 1):
                # Extract key fields - PRIORITIZE requested fields first!
                key_fields = {}
                other_fields = {}
                
                # First pass: Always include requested fields (even if NULL)
                for key in requested_fields:
                    if key in record:
                        key_fields[key] = record[key]  # Include even if None
                
                # Second pass: Include other non-null fields (up to limit)
                for key, value in record.items():
                    # Skip if already included, or if it's an embedding/technical field
                    if key in key_fields or 'embedding' in key.lower() or key in ['date_created', 'date_modified']:
                        continue
                    # Skip null values (unless it's a requested field, which we already handled)
                    if value is None:
                        continue
                    # Include up to 15 total fields (increased from 10 to accommodate requested fields)
                    if len(key_fields) + len(other_fields) < 15:
                        other_fields[key] = value
                
                # Merge: requested fields first, then others
                key_fields = {**key_fields, **other_fields}
                
                summary_parts.append(f"  Record {i}: {json.dumps(key_fields, default=str)}")
            
            if len(v2_data) > 3:
                summary_parts.append(f"  ... and {len(v2_data) - 3} more records")
        
        # V1 Data (MongoDB) - ALWAYS use V1 label!
        if v1_data:
            summary_parts.append(f"\n**V1 Data (MongoDB):** {len(v1_data)} documents")
            
            # Show first few documents
            preview = v1_data[:3]
            for i, doc in enumerate(preview, 1):
                # Extract key fields (limit to avoid token overflow)
                key_fields = {}
                for key, value in doc.items():
                    if value is None or key == '_id':
                        continue
                    if len(key_fields) < 10:
                        key_fields[key] = value
                
                summary_parts.append(f"  Document {i}: {json.dumps(key_fields, default=str)}")
            
            if len(v1_data) > 3:
                summary_parts.append(f"  ... and {len(v1_data) - 3} more documents")
        
        return "\n".join(summary_parts)
    
    def _fallback_formatting(
        self,
        user_question: str,
        query_results: Dict[str, List[Dict]]
    ) -> str:
        """Fallback formatting when LLM unavailable or disabled - ALWAYS use V2/V1 terminology!"""
        
        v2_data = query_results.get('postgresql', [])
        v1_data = query_results.get('mongodb', [])
        total = len(v2_data) + len(v1_data)
        
        if total == 0:
            return "I didn't find any matching records. Try adjusting your search criteria."
        
        # Simple template-based response with V2/V1 labels
        response = f"I found {total} result(s) for your query.\n\n"
        
        if v2_data:
            response += f"### 🗄️ V2 Data (PostgreSQL): {len(v2_data)} records\n\n"
            # Use existing formatting logic here (from nodes.py)
        
        if v1_data:
            response += f"\n### 🍃 V1 Data (MongoDB): {len(v1_data)} documents\n\n"
        
        return response
    
    def _generate_cache_key(
        self,
        user_question: str,
        query_results: Dict[str, List[Dict]]
    ) -> str:
        """Generate cache key for narrative"""
        import hashlib
        
        # Create hash from question + result count + first record IDs
        v2_count = len(query_results.get('postgresql', []))
        v1_count = len(query_results.get('mongodb', []))
        
        # Include first record ID if available
        first_id = ""
        if query_results.get('postgresql'):
            first_record = query_results['postgresql'][0]
            first_id = str(first_record.get('crime_id') or first_record.get('person_id') or '')
        
        cache_string = f"{user_question}_{v2_count}_{v1_count}_{first_id}"
        cache_hash = hashlib.md5(cache_string.encode()).hexdigest()[:12]
        
        return cache_hash
    
    def enable(self):
        """Enable narrative formatting"""
        self.enabled = True
        logger.info("Narrative formatting enabled")
    
    def disable(self):
        """Disable narrative formatting (use fallback)"""
        self.enabled = False
        logger.info("Narrative formatting disabled")



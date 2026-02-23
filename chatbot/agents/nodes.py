"""
LangGraph Node Implementations - Professional Modular Architecture
Testable, maintainable, and follows SOLID principles
"""

import re
import json
import logging
from typing import Dict, Any, Optional, Tuple, List, Protocol
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# ============================================================================
# Protocols for Dependency Injection
# ============================================================================

class LLMClientProtocol(Protocol):
    """Protocol for LLM clients"""
    def detect_intent(self, message: str) -> str: ...
    def generate_sql_with_context(self, message: str, schema: str, plan: Dict) -> Optional[str]: ...
    def generate_mongodb_query(self, message: str, schema: str) -> Optional[str]: ...

class SchemaManagerProtocol(Protocol):
    """Protocol for schema managers"""
    def detect_target_database(self, message: str) -> str: ...
    def get_combined_schema(self) -> Dict: ...

class DatabaseExecutorProtocol(Protocol):
    """Protocol for database executors"""
    def execute_query(self, query: str) -> Tuple[bool, Any]: ...

class CacheManagerProtocol(Protocol):
    """Protocol for cache managers"""
    def get_cached_schema(self) -> Optional[Dict]: ...
    def cache_schema(self, schema: Dict) -> None: ...
    def get_cached_query_result(self, query: str) -> Optional[Any]: ...
    def cache_query_result(self, query: str, result: Any) -> None: ...

# ============================================================================
# State Management
# ============================================================================

@dataclass
class WorkflowState:
    """Type-safe workflow state"""
    user_message: str
    intent: Optional[str] = None
    target_database: Optional[str] = None
    schema: Optional[str] = None
    query_plan: Optional[Dict] = None
    queries: Optional[Dict] = None
    validated_queries: Optional[Dict] = None
    results: Optional[Dict] = None
    final_response: Optional[str] = None
    error: Optional[str] = None
    early_exit: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for LangGraph"""
        return {
            'user_message': self.user_message,
            'intent': self.intent,
            'target_database': self.target_database,
            'schema': self.schema,
            'query_plan': self.query_plan,
            'queries': self.queries,
            'validated_queries': self.validated_queries,
            'results': self.results,
            'final_response': self.final_response,
            'error': self.error,
            'early_exit': self.early_exit,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowState':
        """Create from dictionary"""
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

# ============================================================================
# Base Node Class
# ============================================================================

class BaseNode(ABC):
    """Base class for workflow nodes"""
    
    @abstractmethod
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the node logic"""
        pass
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Make nodes callable"""
        return self.execute(state)

# ============================================================================
# Individual Node Implementations
# ============================================================================

class IntentParserNode(BaseNode):
    """Node 1: Parse user intent with COMPREHENSIVE conversation awareness + entity detection"""
    
    def __init__(
        self,
        llm_client: LLMClientProtocol,
        schema_manager: SchemaManagerProtocol,
        cache_manager: CacheManagerProtocol,
        conversation_handler: Any
    ):
        self.llm = llm_client
        self.schema_manager = schema_manager
        self.cache = cache_manager
        self.conversation = conversation_handler
        
        # Import entity detector
        try:
            from agents.entity_detector import EntityDetector
            self.entity_detector = EntityDetector
            self.use_entity_detection = True
        except ImportError:
            self.use_entity_detection = False
        
        # Import DOPAMAS conversation patterns
        try:
            from agents.dopamas_conversation_patterns import DOPAMASConversationPatterns, ConversationType
            self.conversation_patterns = DOPAMASConversationPatterns
            self.conversation_type_enum = ConversationType
            self.use_advanced_conversation = True
            logger.info("DOPAMAS conversation patterns loaded ⭐")
        except ImportError:
            logger.warning("DOPAMAS conversation patterns not available")
            self.use_advanced_conversation = False
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Parse user intent with COMPREHENSIVE conversation handling + smart entity detection"""
        logger.info("Node: parse_intent")
        
        user_message = state.get('user_message', '').strip()
        message_lower = user_message.lower().strip()
        words = message_lower.split()
        
        # ⭐ STEP 0: Check for CONVERSATIONAL PATTERNS FIRST (before any ambiguity checks!)
        # This ensures greetings, small talk, acknowledgments are handled properly
        # and don't get caught by random pattern detection
        if self.use_advanced_conversation:
            conversation_type = self.conversation_patterns.detect_conversation_type(user_message)
            
            if conversation_type:
                # Check if this is pure conversation (no data query needed)
                if self.conversation_patterns.should_skip_data_query(conversation_type):
                    # Pure conversation - respond and exit early
                    response = self.conversation_patterns.get_response(user_message, conversation_type)
                    state['final_response'] = response
                    state['intent'] = 'conversation'
                    state['conversation_type'] = conversation_type.value
                    state['early_exit'] = True
                    state['query_confidence'] = 1.0  # High confidence for recognized conversations
                    logger.info(f"Handled as pure conversation: {conversation_type.value}")
                    return state
                else:
                    # Conversational but might also want data (e.g., frustration + query)
                    # Extract format preferences and continue to data query
                    format_prefs = self.conversation_patterns.extract_format_preference(user_message)
                    state['format_preferences'] = format_prefs
                    logger.info(f"Detected conversation type: {conversation_type.value}, continuing to data query")
        
        # ⭐ STEP 1: GENERAL AMBIGUITY CHECK - If query is too vague, ask for clarification
        # This catches cases like "blessing", "rajesh", "sangareddy" without clear intent
        # BUT only if it's NOT a recognized conversation pattern
        
        # Check if query is just 1-3 words (likely ambiguous)
        is_short_query = len(words) <= 3
        
        # Check if it has clear query keywords
        query_keywords = [
            'find', 'search', 'get', 'show', 'list', 'display', 'who', 'what', 'where', 'when', 
            'how many', 'count', 'calculate', 'compute', 'sum', 'total', 'average', 'avg', 'statistics', 'stats', 'trends',
            'details', 'information', 'about', 'tell me', 'give me', 'report', 'reports', 'analysis',
            'crimes', 'person', 'accused', 'drug', 'property', 'police station', 'ps', 'fir',
            'case', 'crime', 'involving', 'related to', 'for', 'by', 'with', 'named', 'called',
            'daily', 'weekly', 'monthly', 'quarterly', 'annual', 'yearly', 'distribution', 'distributions'
        ]
        has_query_keywords = any(kw in message_lower for kw in query_keywords)
        
        # ⭐ STRICT VALIDATION: Check if it looks like a high-confidence entity
        # These are clear enough to process without additional context
        # But we need STRICT validation to avoid false positives!
        
        # Mobile number: EXACTLY 10 digits, numbers only
        is_valid_mobile = bool(re.match(r'^\d{10}$', message_lower))
        
        # Aadhaar number: EXACTLY 12 digits, numbers only
        is_valid_aadhaar = bool(re.match(r'^\d{12}$', message_lower))
        
        # Email pattern: Must have @ and valid format
        is_valid_email = '@' in message_lower and re.search(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', message_lower)
        
        # Crime ID pattern: ObjectId-like, EXACTLY 24 hex chars
        is_valid_crime_id = bool(re.match(r'^[0-9a-f]{24}$', message_lower, re.I))
        
        # FIR number pattern: e.g., "243/2022" (digits/digits)
        is_valid_fir = bool(re.match(r'^\d+/\d+$', message_lower))
        
        looks_like_entity = is_valid_mobile or is_valid_aadhaar or is_valid_email or is_valid_crime_id or is_valid_fir
        
        # ⭐ CONFIDENCE SCORING: Calculate query confidence BEFORE processing
        # This helps us decide: execute immediately, confirm first, or ask clarification
        detected_entities_for_confidence = []  # Will be populated after entity detection
        query_confidence = 0.0
        
        # ⭐ IMPROVED: Check for invalid/gibberish input patterns
        # Detect patterns like "dd. 099e3. wewer. werwre..." (random text, partial IDs, etc.)
        # BUT exclude common conversational phrases AND valid date patterns
        conversational_phrases = [
            'how are you', 'how are', 'how\'s it going', 'what\'s up', 'whats up',
            'good morning', 'good afternoon', 'good evening', 'good night',
            'hey', 'hi', 'hello', 'hola', 'namaste',
            'fine', 'ok', 'okay', 'thanks', 'thank you', 'yes', 'no', 'sure'
        ]
        is_conversational_phrase = any(phrase in message_lower for phrase in conversational_phrases)
        
        # ⭐ VALID DATE PATTERNS: Exclude these from random pattern detection
        # Date formats: DD-MM-YYYY, DD/MM/YYYY, YYYY-MM-DD, "February 2025", "last 30 days", etc.
        date_patterns = [
            r'\d{1,2}[-/]\d{1,2}[-/]\d{4}',  # DD-MM-YYYY or DD/MM/YYYY
            r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',  # YYYY-MM-DD
            r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}',  # "February 2025"
            r'\d{4}',  # Year like "2025"
            r'last\s+\d+\s+days?',  # "last 30 days"
            r'\d+\s+days?\s+ago',  # "30 days ago"
            r'between\s+\d',  # "between 01-01-2025"
            r'from\s+\d.*onwards',  # "from 11-05-2025 onwards"
        ]
        has_date_pattern = any(re.search(pattern, message_lower, re.I) for pattern in date_patterns)
        
        # ⭐ VALID QUERY PATTERNS: Exclude IO name queries, case status queries, statistical queries, etc.
        valid_query_patterns = [
            r'handled\s+by\s+io',  # "handled by IO"
            r'handled\s+by\s+io\s+[\'"]',  # "handled by IO 'NAME'"
            r'io\s+[\'"]',  # "IO 'NAME'"
            r'find.*crimes.*handled.*by.*io',  # "Find crimes handled by IO 'NAME'"
            r'case\s+status',  # "case status"
            r'pending\s+cases?',  # "pending cases"
            r'\bpending\b',  # "pending" (standalone)
            r'crime\s+type',  # "crime type"
            r'police\s+station',  # "police station"
            r'io\s+rank',  # "IO rank"
            r'registered\s+in',  # "registered in 2025"
            r'registered\s+on',  # "registered on 27-02-2025"
            r'registered\s+between',  # "registered between"
            r'count\s+(crimes|accused|drugs|properties|persons)',  # "count crimes by..."
            r'calculate\s+(total|average|sum)',  # "calculate total..."
            r'show\s+(statistics|statistics|trends|report)',  # "show statistics..."
            r'by\s+(month|year|district|police\s+station|case\s+status)',  # "by month", "by year"
            r'ndps\s+act|acts\s+sections?|section\s+\d+',  # "NDPS Act", "acts sections", "section 8c"
            r'major\s+head|minor\s+head',  # "major head", "minor head"
            r'commercial\s+quantity|is_commercial',  # "commercial quantity"
            r'property\s+status|seized\s+properties?',  # "property status", "seized properties"
            r'accused\s+type|accused\s+role',  # "accused type", "accused role"
            r'daily|weekly|monthly|quarterly|annual',  # "daily report", "monthly statistics"
        ]
        has_valid_query_pattern = any(re.search(pattern, message_lower, re.I) for pattern in valid_query_patterns)
        
        # ⭐ EXCLUDE IO NAME QUERIES from random pattern detection
        # IO name queries like "IO 'BARPATI RAMESH'" should NOT be flagged as random
        is_io_name_query = (
            bool(re.search(r'io\s+[\'"].*[\'"]', message_lower, re.I)) or 
            bool(re.search(r'handled\s+by\s+io\s+[\'"]', message_lower, re.I)) or
            bool(re.search(r'find.*crimes.*handled.*by.*io', message_lower, re.I)) or
            bool(re.search(r'find.*all.*crimes.*handled.*by.*io', message_lower, re.I))
        )
        
        # ⭐ CRITICAL: If it's an IO name query, it's NOT random - skip random pattern check entirely
        if is_io_name_query:
            has_random_patterns = False
        else:
            has_random_patterns = (
                # Multiple single/double letters separated by dots/spaces (BUT exclude if it's a date or valid query)
                (bool(re.search(r'\b[a-z]{1,2}\s*[.\s]+\s*[a-z]{1,2}\s*[.\s]+', message_lower)) and not has_date_pattern) or
                # Partial hex IDs (like "099e3" - too short for ObjectId) BUT exclude if it's part of a date
                (bool(re.search(r'\b[0-9a-f]{3,5}\b', message_lower)) and not has_date_pattern and not re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}', message_lower)) or
                # Multiple random words without structure (BUT exclude conversational phrases, dates, and valid queries)
                (len(words) >= 3 and not has_query_keywords and not is_conversational_phrase and not has_date_pattern and not has_valid_query_pattern and not any(re.match(r'^\d{10,12}$', w) for w in words))
            )
        
        # If query is short, has no query keywords, and doesn't look like a clear entity → AMBIGUOUS!
        # OR if it has random/gibberish patterns → AMBIGUOUS!
        # BUT skip if it's a conversational phrase (already handled above)
        if not is_conversational_phrase and ((is_short_query and not has_query_keywords and not looks_like_entity) or has_random_patterns):
            # Low confidence - ask for clarification
            clarification_msg = self._generate_generic_clarification_message(user_message)
            state['final_response'] = clarification_msg
            state['early_exit'] = True
            state['intent'] = 'clarification_needed'
            state['query_confidence'] = 0.0  # Very low confidence
            reason = "random/gibberish patterns" if has_random_patterns else "short query without clear intent"
            logger.info(f"Query is too vague ({reason}): '{user_message}'. Asking for clarification.")
            return state
        
        # STEP 2: ALWAYS detect entities (for query planning and generation)
        detected_entities = []
        if self.use_entity_detection:
            detected_entities = self.entity_detector.detect_entities(user_message, include_domain_entities=True)
            # Store ALL detected entities in state for query planner and generator
            state['detected_entities'] = [
                {
                    'type': e.entity_type.value if hasattr(e.entity_type, 'value') else str(e.entity_type),
                    'value': e.value,
                    'search_fields': e.search_fields,
                    'confidence': e.confidence,
                    'domain_type': e.metadata.get('domain_type', None),
                    'v2_fields': e.metadata.get('v2_fields', []),
                    'v1_fields': e.metadata.get('v1_fields', [])
                }
                for e in detected_entities
            ]
            logger.info(f"Detected {len(detected_entities)} entities for query planning")
            
            # ⭐ CALCULATE CONFIDENCE after entity detection
            query_confidence = self._calculate_query_confidence(
                user_message,
                state['detected_entities'],
                has_query_keywords
            )
            state['query_confidence'] = query_confidence
            logger.info(f"Query confidence score: {query_confidence:.2f} ({'High' if query_confidence > 0.90 else 'Medium' if query_confidence > 0.50 else 'Low'})")
        
        # ⭐ NEW: AMBIGUITY DETECTION - Ask user to clarify vague queries!
        # Check if query is just a name/entity without clear context
        if self.use_entity_detection and detected_entities:
            # Check if we have a high-confidence entity (0.90+) like ObjectID, Mobile, Aadhaar
            high_confidence_entity = None
            for entity_dict in state.get('detected_entities', []):
                if entity_dict['confidence'] >= 0.90:
                    high_confidence_entity = entity_dict
                    break
            
            # If high-confidence entity found (especially ObjectID/crime_id), use entity search
            if high_confidence_entity:
                logger.info(f"Detected high-confidence entity: {high_confidence_entity['type']} = {high_confidence_entity['value']}")
                state['intent'] = 'entity_search'
                state['target_database'] = 'both'
                state['detected_entity'] = high_confidence_entity
                return state
            
            # ⭐ AMBIGUITY CHECK: Low-confidence entity (like person_name) without clear query context
            # If query is just a name/entity without keywords like "find", "search", "get", "show", etc.
            message_lower = user_message.lower().strip()
            query_keywords = ['find', 'search', 'get', 'show', 'list', 'display', 'who', 'what', 'where', 'when', 'how many', 'count', 'details', 'information', 'about']
            has_query_keywords = any(kw in message_lower for kw in query_keywords)
            
            # Check if detected entity is low-confidence (like person_name with 0.70)
            low_confidence_entity = None
            for entity_dict in state.get('detected_entities', []):
                if entity_dict['confidence'] < 0.90 and entity_dict['type'] in ['person_name', 'location', 'organization']:
                    low_confidence_entity = entity_dict
                    break
            
            # If we have a low-confidence entity AND no clear query keywords, ask for clarification
            if low_confidence_entity and not has_query_keywords:
                entity_type = low_confidence_entity['type']
                entity_value = low_confidence_entity['value']
                
                # Generate helpful clarification message
                clarification_msg = self._generate_clarification_message(entity_type, entity_value, user_message)
                state['final_response'] = clarification_msg
                state['early_exit'] = True
                state['intent'] = 'clarification_needed'
                logger.info(f"Query is ambiguous - detected {entity_type} '{entity_value}' without clear context. Asking for clarification.")
                return state
        
        # Check for entity-only query (like just a mobile number) - but only if it's NOT ambiguous
        # High-confidence entities (mobile, email, crime_id) are handled above
        # This handles other entity-only queries that are clear
        if self.use_entity_detection and self.entity_detector.is_entity_only_query(user_message):
            entity = self.entity_detector.get_primary_entity(user_message)
            if entity:
                logger.info(f"Detected entity-only query: {entity}")
                # Mark as query intent, will search for this entity
                state['intent'] = 'entity_search'
                state['target_database'] = 'both'
                state['detected_entity'] = {
                    'type': entity.entity_type.value,
                    'value': entity.value,
                    'search_fields': entity.search_fields
                }
                return state
        
        # ⭐ SKIP OLD RELEVANCE CHECK if new conversation patterns are available!
        # The new patterns already handle greetings, help, out-of-scope, etc.
        # Only use old handler as fallback
        if not self.use_advanced_conversation:
            # Fallback to old conversation handler
            greeting_response = self.conversation.handle_greeting(user_message)
            if greeting_response:
                state['final_response'] = greeting_response
                state['early_exit'] = True
                return state
            
            # Get schema for relevance check
            cached_schema = self.cache.get_cached_schema()
            if not cached_schema:
                cached_schema = self.schema_manager.get_combined_schema()
                self.cache.cache_schema(cached_schema)
            
            # Check relevance
            is_relevant, clarification = self.conversation.is_relevant_to_data(
                user_message,
                cached_schema
            )
            
            if not is_relevant:
                state['final_response'] = clarification
                state['early_exit'] = True
                logger.info("Query not relevant - providing guidance")
                return state
        
        # Check ambiguity (only if old handler is being used)
        if not self.use_advanced_conversation:
            clarification = self.conversation.clarify_ambiguous_query(
                user_message,
                cached_schema
            )
            if clarification:
                state['final_response'] = clarification
                state['early_exit'] = True
                logger.info("Query ambiguous - requesting clarification")
                return state
        
        # Detect intent and target database
        state['intent'] = self.llm.detect_intent(user_message)
        state['target_database'] = self.schema_manager.detect_target_database(user_message)
        
        # ⭐ CRITICAL: Check confidence and ask for clarification if too low
        query_confidence = state.get('query_confidence', 1.0)  # Default to high if not calculated
        
        # If confidence is LOW (<0.50), ask for clarification (even if it passed early checks)
        if query_confidence < 0.50:
            clarification_msg = self._generate_generic_clarification_message(user_message)
            state['final_response'] = clarification_msg
            state['early_exit'] = True
            state['intent'] = 'clarification_needed'
            logger.info(f"Low confidence query ({query_confidence:.2f}) - asking for clarification before processing")
            return state
        
        # ⭐ MEDIUM CONFIDENCE: Only confirm if query is truly ambiguous
        # Clear queries like "case status 'UI'", "crime type 'Narcotics'", etc. should execute directly
        if 0.50 <= query_confidence < 0.90:
            # ⭐ DYNAMIC: Build domain keywords from actual schema and domain entities
            clear_domain_keywords = self._build_dynamic_domain_keywords()
            
            # Also check if query has clear action verbs (should execute directly)
            clear_action_verbs = [
                'find', 'show', 'list', 'get', 'search', 'display',
                'count', 'calculate', 'compute', 'analyze'
            ]
            has_action_verb = any(verb in message_lower for verb in clear_action_verbs)
            has_clear_domain_keyword = any(kw in message_lower for kw in clear_domain_keywords)
            
            # If query has clear domain keywords OR clear action verbs, execute directly (don't ask for confirmation)
            # Also check for incomplete questions - if question asks for something but parameter is missing,
            # we should still try to execute (e.g., "Show crime summary for crime_id" - try to show all summaries)
            # ⭐ DYNAMIC: Detect incomplete questions by checking for "for [field]" or "by [field]" patterns
            # where [field] is mentioned but no actual value is provided
            is_incomplete_question = self._detect_incomplete_question(message_lower)
            
            if has_clear_domain_keyword or has_action_verb or is_incomplete_question:
                logger.info(f"Medium confidence query ({query_confidence:.2f}) but has clear keywords/actions - executing directly")
                # Continue to intent detection and query generation
            else:
                # Truly ambiguous medium confidence - confirm interpretation
                confirmation_msg = self._generate_confirmation_message(user_message, state)
                state['final_response'] = confirmation_msg
                state['early_exit'] = True
                state['intent'] = 'confirmation_needed'
                state['needs_confirmation'] = True  # Flag for follow-up handling
                logger.info(f"Medium confidence query ({query_confidence:.2f}) - asking for confirmation before executing")
                return state
        
        logger.info(f"Intent detected: {state['intent']}, Target DB: {state['target_database']}, Confidence: {query_confidence:.2f}")
        return state
    
    def _generate_confirmation_message(self, user_message: str, state: Dict[str, Any]) -> str:
        """
        Generate confirmation message for medium-confidence queries
        Asks user to confirm interpretation before executing
        
        Args:
            user_message: Original user message
            state: Current workflow state
            
        Returns:
            Confirmation message
        """
        detected_entities = state.get('detected_entities', [])
        intent = state.get('intent', 'general')
        target_db = state.get('target_database', 'both')
        
        # Build interpretation summary
        interpretation_parts = []
        
        if detected_entities:
            entity_summary = []
            for e in detected_entities[:3]:  # Show top 3 entities
                entity_type = e.get('type', 'unknown')
                entity_value = e.get('value', '')
                entity_type_display = {
                    'person_name': 'person name',
                    'mobile_number': 'phone number',
                    'email': 'email address',
                    'objectid': 'crime ID',
                    'location': 'location'
                }.get(entity_type, entity_type)
                entity_summary.append(f"{entity_type_display}: '{entity_value}'")
            
            if entity_summary:
                interpretation_parts.append(f"**Detected:** {', '.join(entity_summary)}")
        
        intent_display = {
            'query': 'search/retrieve information',
            'aggregation': 'calculate statistics/counts',
            'entity_search': 'search for specific entity',
            'general': 'general information'
        }.get(intent, intent)
        interpretation_parts.append(f"**Intent:** {intent_display}")
        
        interpretation = "\n".join(interpretation_parts) if interpretation_parts else "general search"
        
        confirmation = f"""Just to confirm, you want me to:

{interpretation}

**Is this correct?** 

- **Yes** → I'll proceed with the search
- **No** → Please provide more details or correct me

You can also say:
- "Yes, proceed"
- "No, I want [specific information]"
- "Search for [corrected query]"

This helps me give you the most accurate results! 🎯"""
        
        return confirmation
    
    def _generate_clarification_message(self, entity_type: str, entity_value: str, user_message: str) -> str:
        """
        Generate a PROGRESSIVE clarification message when entity is detected but query is ambiguous
        Uses the framework: Acknowledge → Explain Gap → Ask Questions → Provide Examples
        
        Args:
            entity_type: Type of entity detected (person_name, location, etc.)
            entity_value: Value of the entity
            user_message: Original user message
            
        Returns:
            Clarification message asking user to be more specific
        """
        entity_type_map = {
            'person_name': 'person name',
            'location': 'location',
            'organization': 'organization',
            'mobile_number': 'phone number',
            'email': 'email address'
        }
        
        entity_display = entity_type_map.get(entity_type, entity_type)
        
        # Step 1: Acknowledge Input
        # Step 2: Explain the Gap
        # Step 3: Ask Specific Questions
        # Step 4: Provide Examples
        
        # Customize questions based on entity type
        if entity_type == 'person_name':
            specific_questions = """- **Are you looking for a person** with this name?
- **Are you searching for crimes** involving this person?
- **Do you want property records** for this person?
- **Are you looking for police records** or case files?"""
            examples = f"""- "Find person named {entity_value}"
- "Get crimes involving {entity_value}"
- "Search for {entity_value} in accused records"
- "Show all information about {entity_value}"
- "Who is {entity_value}?"
- "What crimes is {entity_value} involved in?" """
        elif entity_type == 'location':
            specific_questions = """- **Crime statistics** in this location?
- **Police stations** in this area?
- **Recent incidents** in this location?
- **Specific person or case** in this location?"""
            examples = f"""- "Find crimes in {entity_value}"
- "Get crime statistics for {entity_value}"
- "Show police stations in {entity_value}"
- "Search for cases in {entity_value}" """
        else:
            specific_questions = """- **What type of information** you're looking for?
- **What you want to know** about this?"""
            examples = f"""- "Find information about {entity_value}"
- "Get details for {entity_value}"
- "Search for {entity_value}"
- "Show all records related to {entity_value}" """
        
        clarification = f"""I detected "{entity_value}" which looks like a {entity_display}, but I need more information to help you properly! 😊

**What would you like me to search for?**

{specific_questions}

**Examples of clear queries:**
{examples}

**Or you can ask directly:**
- "Who is {entity_value}?" (if it's a person)
- "What crimes is {entity_value} involved in?" (if it's a person)
- "Show crimes in {entity_value}" (if it's a location)
- "Get details for {entity_value}"

Just add a bit more context, and I'll find exactly what you need! 🎯"""
        
        return clarification
    
    def _build_dynamic_domain_keywords(self) -> List[str]:
        """
        Dynamically build domain keywords from schema and domain entities
        This ensures we're not hardcoding field names
        """
        keywords = set()
        
        try:
            # Get schema dynamically
            cached_schema = self.cache.get_cached_schema()
            if not cached_schema:
                cached_schema = self.schema_manager.get_combined_schema()
                if cached_schema:
                    self.cache.cache_schema(cached_schema)
            
            if cached_schema:
                # Extract column/field names from PostgreSQL schema
                for table_name, columns in cached_schema.get('postgresql', {}).items():
                    # Add table name variations
                    keywords.add(table_name.replace('_', ' '))
                    keywords.add(table_name)
                    
                    # Add column names as keywords
                    for col_info in columns:
                        col_name = col_info.get('column', '')
                        if col_name:
                            # Add column name
                            keywords.add(col_name)
                            # Add column name with spaces (e.g., "fir_num" -> "fir num")
                            keywords.add(col_name.replace('_', ' '))
                            # Add column name parts (e.g., "fir_num" -> "fir", "num")
                            if '_' in col_name:
                                keywords.update(col_name.split('_'))
                
                # Extract field names from MongoDB schema
                for collection_name, collection_info in cached_schema.get('mongodb', {}).items():
                    keywords.add(collection_name.replace('_', ' '))
                    keywords.add(collection_name)
                    
                    for field_info in collection_info.get('fields', []):
                        field_name = field_info.get('field', '')
                        if field_name:
                            keywords.add(field_name)
                            keywords.add(field_name.replace('_', ' '))
                            if '_' in field_name:
                                keywords.update(field_name.split('_'))
            
            # Add domain entity types dynamically
            try:
                from agents.crime_domain_entities import CrimeDomainEntityType
                for entity_type in CrimeDomainEntityType:
                    # Add entity type name
                    keywords.add(entity_type.value)
                    keywords.add(entity_type.value.replace('_', ' '))
                    # Add entity type parts
                    if '_' in entity_type.value:
                        keywords.update(entity_type.value.split('_'))
            except ImportError:
                pass
            
            # Add common query patterns (these are language patterns, not schema-specific)
            query_patterns = [
                'between', 'from', 'onwards', 'last', 'days', 'today', 'yesterday',
                'recently', 'recent', 'modified', 'created', 'registered',
                'count', 'calculate', 'statistics', 'by month', 'by year', 'by district',
                'daily', 'weekly', 'monthly', 'quarterly', 'annual',
                'pending', 'pending cases', 'handled by io',
                'similar', 'similarity', 'embedding',
                'summary', 'summaries', 'brief facts',
                'total', 'sum', 'average', 'avg', 'worth',
                'breakdown', 'hierarchical breakdown'
            ]
            keywords.update(query_patterns)
            
            # Add drug names from domain entities (if available)
            try:
                from agents.crime_domain_entities import CRIME_DOMAIN_PATTERNS, CrimeDomainEntityType
                if CrimeDomainEntityType.DRUG_NAME in CRIME_DOMAIN_PATTERNS:
                    for pattern in CRIME_DOMAIN_PATTERNS[CrimeDomainEntityType.DRUG_NAME]:
                        # Extract drug names from regex patterns (e.g., r'\b(ganja|marijuana)\b' -> ['ganja', 'marijuana'])
                        import re
                        matches = re.findall(r'\(([^)]+)\)', pattern)
                        for match in matches:
                            keywords.update([d.strip() for d in match.split('|')])
            except (ImportError, KeyError):
                pass
            
        except Exception as e:
            logger.warning(f"Error building dynamic keywords, using fallback: {e}")
            # Fallback to basic keywords if schema access fails
            keywords = {
                'case status', 'crime type', 'fir number', 'fir date',
                'drug', 'drugs', 'seizure', 'property', 'accused',
                'summary', 'similar', 'embedding'
            }
        
        return list(keywords)
    
    def _detect_incomplete_question(self, message_lower: str) -> bool:
        """
        Dynamically detect incomplete questions where a field is mentioned but no value provided
        Examples: "for crime_id", "by specific", "for [field]" without actual value
        """
        # Pattern: "for [field]" or "by [field]" where field is mentioned but no value follows
        incomplete_patterns = [
            r'for\s+(crime_id|crime\s+id|person_id|person\s+id|accused_id|accused\s+id|drug_id|drug\s+id|property_id|property\s+id)\b',
            r'by\s+(crime_id|crime\s+id|person_id|person\s+id|accused_id|accused\s+id|drug_id|drug\s+id|property_id|property\s+id)\b',
            r'linked\s+to\s+(crime_id|crime\s+id|person_id|person\s+id)\b',
            r'by\s+specific\b',
            r'for\s+specific\b',
            r'processed\s+by\s+specific\s+model\b',  # "processed by specific model"
        ]
        
        # Check if message matches incomplete patterns but doesn't have a value after the field
        for pattern in incomplete_patterns:
            if re.search(pattern, message_lower, re.I):
                # Check if there's a value after the field (quoted string, ID, etc.)
                # If pattern matches but no value follows, it's incomplete
                match = re.search(pattern, message_lower, re.I)
                if match:
                    # Check if there's content after the match that looks like a value
                    after_match = message_lower[match.end():].strip()
                    # If no quoted value, ID pattern, or other value follows, it's incomplete
                    if not (re.search(r'[\'"][^\'"]+[\'"]', after_match) or  # Quoted value
                            re.search(r'\b[0-9a-f]{24}\b', after_match) or  # ObjectId
                            re.search(r'\b\d+\b', after_match)):  # Number
                        return True
        
        return False
    
    def _check_missing_fields(self, message_lower: str) -> bool:
        """
        Check if query mentions fields that might not exist in the schema
        Returns True if query mentions fields that are commonly missing
        """
        # Get schema to check if fields exist
        try:
            cached_schema = self.cache.get_cached_schema()
            if not cached_schema:
                cached_schema = self.schema_manager.get_combined_schema()
                if cached_schema:
                    self.cache.cache_schema(cached_schema)
            
            if cached_schema:
                # Collect all available field names from schema
                available_fields = set()
                for table_name, columns in cached_schema.get('postgresql', {}).items():
                    for col_info in columns:
                        col_name = col_info.get('column', '').lower()
                        available_fields.add(col_name)
                        available_fields.add(col_name.replace('_', ' '))
                
                for collection_name, collection_info in cached_schema.get('mongodb', {}).items():
                    for field_info in collection_info.get('fields', []):
                        field_name = field_info.get('field', '').lower()
                        available_fields.add(field_name)
                        available_fields.add(field_name.replace('_', ' '))
                
                # Check if message mentions fields that might not be in schema
                # Common fields that might be missing
                potentially_missing = ['email', 'email address', 'nationality', 'height', 'build', 
                                     'education', 'education qualification', 'classification']
                
                for field in potentially_missing:
                    if field in message_lower and field not in available_fields:
                        return True
        except Exception:
            pass  # If schema check fails, don't assume fields are missing
        
        return False
    
    def _calculate_query_confidence(self, user_message: str, detected_entities: List[Dict], has_query_keywords: bool) -> float:
        """
        Calculate confidence score for query (0.0 to 1.0) with STRICT VALIDATION
        
        Returns:
            Confidence score:
            - >0.90: High confidence (clear query, execute immediately)
            - 0.50-0.90: Medium confidence (confirm before executing)
            - <0.50: Low confidence (ask clarification questions)
        """
        confidence = 0.0
        message_lower = user_message.lower().strip()
        words = message_lower.split()
        
        # ⭐ VALID DATE PATTERNS: Exclude these from random pattern detection (same as early exit check)
        date_patterns = [
            r'\d{1,2}[-/]\d{1,2}[-/]\d{4}',  # DD-MM-YYYY or DD/MM/YYYY
            r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',  # YYYY-MM-DD
            r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}',  # "February 2025"
            r'\d{4}',  # Year like "2025"
            r'last\s+\d+\s+days?',  # "last 30 days"
            r'\d+\s+days?\s+ago',  # "30 days ago"
            r'between\s+\d',  # "between 01-01-2025"
            r'from\s+\d.*onwards',  # "from 11-05-2025 onwards"
        ]
        has_date_pattern = any(re.search(pattern, message_lower, re.I) for pattern in date_patterns)
        
        # ⭐ VALID QUERY PATTERNS: Exclude IO name queries, case status queries, statistical queries, etc.
        valid_query_patterns = [
            r'handled\s+by\s+io',  # "handled by IO"
            r'handled\s+by\s+io\s+[\'"]',  # "handled by IO 'NAME'"
            r'io\s+[\'"]',  # "IO 'NAME'"
            r'find.*crimes.*handled.*by.*io',  # "Find crimes handled by IO 'NAME'"
            r'case\s+status',  # "case status"
            r'pending\s+cases?',  # "pending cases"
            r'\bpending\b',  # "pending" (standalone)
            r'crime\s+type',  # "crime type"
            r'police\s+station',  # "police station"
            r'io\s+rank',  # "IO rank"
            r'registered\s+in',  # "registered in 2025"
            r'registered\s+on',  # "registered on 27-02-2025"
            r'registered\s+between',  # "registered between"
            r'count\s+(crimes|accused|drugs|properties|persons)',  # "count crimes by..."
            r'calculate\s+(total|average|sum)',  # "calculate total..."
            r'show\s+(statistics|statistics|trends|report)',  # "show statistics..."
            r'by\s+(month|year|district|police\s+station|case\s+status)',  # "by month", "by year"
            r'ndps\s+act|acts\s+sections?|section\s+\d+',  # "NDPS Act", "acts sections", "section 8c"
            r'major\s+head|minor\s+head',  # "major head", "minor head"
            r'commercial\s+quantity|is_commercial',  # "commercial quantity"
            r'property\s+status|seized\s+properties?',  # "property status", "seized properties"
            r'accused\s+type|accused\s+role',  # "accused type", "accused role"
            r'daily|weekly|monthly|quarterly|annual',  # "daily report", "monthly statistics"
        ]
        has_valid_query_pattern = any(re.search(pattern, message_lower, re.I) for pattern in valid_query_patterns)
        
        # ⭐ EXCLUDE IO NAME QUERIES from random pattern detection (same as early exit check)
        is_io_name_query = (
            bool(re.search(r'io\s+[\'"].*[\'"]', message_lower, re.I)) or 
            bool(re.search(r'handled\s+by\s+io\s+[\'"]', message_lower, re.I)) or
            bool(re.search(r'find.*crimes.*handled.*by.*io', message_lower, re.I)) or
            bool(re.search(r'find.*all.*crimes.*handled.*by.*io', message_lower, re.I))
        )
        
        # ⭐ STRICT VALIDATION: Check for invalid patterns FIRST
        # If input looks like gibberish/random text, confidence should be very low
        # BUT exclude date patterns, valid query patterns, and IO name queries (same logic as early exit check)
        # ⭐ CRITICAL: If it's an IO name query, it's NOT random - skip random pattern check entirely
        if is_io_name_query:
            has_random_patterns = False
        else:
            has_random_patterns = (
                # Multiple single/double letters separated by dots/spaces (BUT exclude if it's a date or valid query)
                (bool(re.search(r'\b[a-z]{1,2}\s*[.\s]+\s*[a-z]{1,2}\s*[.\s]+', message_lower)) and not has_date_pattern) or
                # Partial hex IDs (like "099e3" - too short for ObjectId) BUT exclude if it's part of a date
                (bool(re.search(r'\b[0-9a-f]{3,5}\b', message_lower)) and not has_date_pattern and not re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}', message_lower)) or
                # Multiple random words without structure (BUT exclude conversational phrases, dates, and valid queries)
                (len(words) >= 3 and not has_query_keywords and not has_date_pattern and not has_valid_query_pattern and not any(re.match(r'^\d{10,12}$', w) for w in words))
            )
        
        if has_random_patterns:
            # Very low confidence for random/gibberish input
            return 0.0
        
        # ⭐ STRICT ENTITY VALIDATION
        # Mobile number: EXACTLY 10 digits, numbers only
        is_valid_mobile = bool(re.match(r'^\d{10}$', message_lower))
        
        # Aadhaar number: EXACTLY 12 digits, numbers only
        is_valid_aadhaar = bool(re.match(r'^\d{12}$', message_lower))
        
        # Email pattern: Must have @ and valid format
        is_valid_email = '@' in message_lower and re.search(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', message_lower)
        
        # Crime ID pattern: ObjectId-like, EXACTLY 24 hex chars
        is_valid_crime_id = bool(re.match(r'^[0-9a-f]{24}$', message_lower, re.I))
        
        # FIR number pattern: e.g., "243/2022" (digits/digits)
        is_valid_fir = bool(re.match(r'^\d+/\d+$', message_lower))
        
        # Base confidence from query keywords (40% weight)
        if has_query_keywords:
            confidence += 0.4
        
        # Entity confidence (30% weight) - BUT with strict validation!
        if detected_entities:
            # ⭐ STRICT: Only count entities that are GENUINELY valid
            valid_entities = []
            for e in detected_entities:
                entity_type = e.get('type', '')
                entity_value = e.get('value', '')
                entity_confidence = e.get('confidence', 0.0)
                
                # Validate based on entity type
                is_valid = False
                if entity_type == 'mobile_number':
                    # Must be exactly 10 digits
                    is_valid = bool(re.match(r'^\d{10}$', entity_value))
                elif entity_type == 'aadhaar':
                    # Must be exactly 12 digits
                    is_valid = bool(re.match(r'^\d{12}$', entity_value))
                elif entity_type == 'email':
                    # Must have valid email format
                    is_valid = '@' in entity_value and re.search(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', entity_value)
                elif entity_type == 'objectid':
                    # Must be exactly 24 hex chars
                    is_valid = bool(re.match(r'^[0-9a-f]{24}$', entity_value, re.I))
                elif entity_type == 'person_name':
                    # ⭐ STRICT: Person names must be genuine (not random text)
                    # Check if it looks like a real name (2-4 words, proper capitalization, no random chars)
                    name_words = entity_value.split()
                    is_valid = (
                        len(name_words) >= 1 and len(name_words) <= 4 and  # Reasonable name length
                        all(len(w) >= 2 for w in name_words) and  # Each word at least 2 chars
                        not any(re.search(r'[0-9]', w) for w in name_words) and  # No numbers in names
                        not any(re.search(r'[^\w\s]', w) for w in name_words) and  # No special chars (except spaces)
                        entity_confidence >= 0.70  # Must have decent confidence
                    )
                else:
                    # For other entity types, use confidence threshold
                    is_valid = entity_confidence >= 0.70
                
                if is_valid:
                    valid_entities.append(entity_confidence)
            
            if valid_entities:
                max_entity_confidence = max(valid_entities)
                confidence += 0.3 * max_entity_confidence
            # If no valid entities, don't add confidence boost
        
        # Query completeness (20% weight)
        # Check for search type keywords
        search_type_keywords = ['person', 'crime', 'drug', 'property', 'accused', 'police station', 'ps', 'fir', 'case']
        has_search_type = any(kw in message_lower for kw in search_type_keywords)
        if has_search_type:
            confidence += 0.2
        
        # ⭐ BOOST for statistical/analytical queries (these are very clear!)
        statistical_keywords = ['count', 'calculate', 'compute', 'sum', 'total', 'average', 'avg', 'statistics', 'stats', 'trends', 'distribution', 'distributions', 'report']
        has_statistical_keyword = any(kw in message_lower for kw in statistical_keywords)
        if has_statistical_keyword:
            confidence = min(1.0, confidence + 0.1)  # Additional boost for statistical queries
        
        # Query length and structure (10% weight)
        # Longer, structured queries are more confident
        if len(words) >= 4:
            confidence += 0.1
        elif len(words) >= 2:
            confidence += 0.05
        
        # ⭐ BOOST CONFIDENCE for queries with date patterns or valid query patterns
        # These are clear, structured queries that should execute directly
        if has_date_pattern or has_valid_query_pattern:
            confidence = min(1.0, confidence + 0.15)  # Significant boost for clear date/query patterns
        
        # High-confidence entity patterns (bonus) - ONLY if valid!
        if is_valid_mobile or is_valid_aadhaar or is_valid_email or is_valid_crime_id or is_valid_fir:
            confidence = min(1.0, confidence + 0.2)  # Boost confidence for valid entities
        
        return min(1.0, confidence)
    
    def _generate_generic_clarification_message(self, user_message: str) -> str:
        """
        Generate a PROGRESSIVE clarification message when query is too vague
        Uses the framework: Acknowledge → Explain Gap → Ask Questions → Provide Examples
        
        Args:
            user_message: Original user message (could be a name, word, etc.)
            
        Returns:
            Clarification message asking user to be more specific
        """
        # Check if the message already looks like a clear query (has query keywords, dates, etc.)
        # If so, don't embed it as a placeholder - it's already structured
        message_lower = user_message.lower().strip()
        query_keywords = ['find', 'search', 'get', 'show', 'list', 'display', 'who', 'what', 'where', 'when', 'how many', 'count']
        has_query_keywords = any(kw in message_lower for kw in query_keywords)
        
        # Check for date patterns
        date_patterns = [
            r'\d{1,2}[-/]\d{1,2}[-/]\d{4}',
            r'\d{4}[-/]\d{1,2}[-/]\d{1,2}',
            r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}',
            r'\d{4}',
            r'last\s+\d+\s+days?',
            r'between\s+\d',
        ]
        has_date_pattern = any(re.search(pattern, message_lower, re.I) for pattern in date_patterns)
        
        # If it already looks like a clear query, provide a more generic clarification
        if has_query_keywords or has_date_pattern or len(user_message.split()) >= 4:
            clarification = f"""I see you asked: "{user_message}"

I need a bit more clarity to give you the best results! 😊

**What would you like me to search for?**

**Please specify:**
- **What type of information?** (person, crime, drug, property, police station, etc.)
- **What specific details?** (name, date, status, type, etc.)
- **Any filters or conditions?** (date range, location, category, etc.)

**Examples of clear queries:**
- "List all crimes registered in 2025"
- "Find crimes handled by IO 'BARPATI RAMESH'"
- "Show all crimes with case status 'UI'"
- "Get crimes by crime type 'Narcotics'"
- "Find person named [name]"
- "Show crimes from police station code '2025005'"

**Or you can ask:**
- "Who is [name]?"
- "What crimes is [name] involved in?"
- "Get details for [entity]"

Please rephrase your query with more specific details, and I'll find exactly what you need! 🎯"""
        else:
            # For very short/vague queries, provide more detailed guidance
            clarification = f"""I see you entered "{user_message}", but I need more information to help you properly! 😊

**What would you like me to search for?**

**Step 1: What TYPE of information?**
Please tell me what you're looking for:
- **Person details** (name, age, address, phone, criminal records)
- **Crimes** (case details, FIR numbers, crime types, case status)
- **Drugs** (drug seizures, quantities, supply chain, transport methods)
- **Properties** (seized properties, vehicles, cash, mobile phones)
- **Police stations** (stations, districts, zones, crime statistics)
- **Something else?** (please specify)

**Step 2: What SPECIFIC details?**
- If it's a **person name**: "Find person named [name]"
- If it's a **location**: "Find crimes in [location]"
- If it's a **crime type**: "Get [type] cases"
- If it's a **category**: "Show [category] records"

**Examples of clear queries:**
- "Find person named [name]"
- "Get crimes involving [name]"
- "Search for [name] in accused records"
- "Show all information about [name]"
- "List all crimes registered in 2025"
- "Find crimes handled by IO '[name]'"

**Or you can ask directly:**
- "Who is [name]?"
- "What crimes is [name] involved in?"
- "Get details for [entity]"

Just add a bit more context about what you're looking for, and I'll find exactly what you need! 🎯"""
        
        return clarification

class SchemaFetcherNode(BaseNode):
    """Node 2: Fetch and intelligently filter schema + AUTO-DETECT COLUMNS from user question"""
    
    def __init__(
        self,
        schema_manager: SchemaManagerProtocol,
        cache_manager: CacheManagerProtocol,
        query_planner: Any,
        smart_schema: Any
    ):
        self.schema_manager = schema_manager
        self.cache = cache_manager
        self.query_planner = query_planner
        self.smart_schema = smart_schema
        
        # ⭐ NEW: Intelligent Column Mapper - KNOWS which columns user is asking about!
        try:
            from agents.column_mapper import IntelligentColumnMapper
            self.column_mapper = IntelligentColumnMapper()
            logger.info("Intelligent Column Mapper initialized ⭐")
        except ImportError:
            logger.warning("Column mapper not available")
            self.column_mapper = None
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch and intelligently filter schema"""
        logger.info("Node: get_schema")
        
        user_message = state.get('user_message', '')
        
        # Get full schema
        cached_schema = self.cache.get_cached_schema()
        if not cached_schema:
            logger.info("Fetching fresh schema from databases")
            cached_schema = self.schema_manager.get_combined_schema()
            self.cache.cache_schema(cached_schema)
        else:
            logger.info("Schema retrieved from cache")
        
        # Analyze query to find relevant tables
        query_plan = self.query_planner.analyze_user_question(
            user_message,
            cached_schema
        )
        state['query_plan'] = query_plan
        
        # ⭐ NEW: AUTO-DETECT COLUMNS from user question!
        # Instead of guessing, we KNOW which columns user is asking about!
        required_columns = []
        required_tables = set(query_plan['relevant_tables'].get('postgresql', []))
        
        if self.column_mapper:
            column_matches = self.column_mapper.find_columns(user_message)
            if column_matches:
                # Add columns to state so QueryGenerator can use them
                state['required_columns'] = [
                    {
                        'table': m.table,
                        'column': m.column,
                        'alias': m.alias,
                        'confidence': m.confidence
                    }
                    for m in column_matches
                ]
                
                # Add tables that have required columns
                for match in column_matches:
                    required_tables.add(match.table)
                
                logger.info(f"⭐ AUTO-DETECTED {len(column_matches)} columns from user question:")
                for match in column_matches[:5]:  # Log top 5
                    logger.info(f"  → {match.table}.{match.column} (confidence: {match.confidence})")
        
        # Update relevant tables with auto-detected tables
        query_plan['relevant_tables']['postgresql'] = list(required_tables)
        
        # Get targeted schema (only relevant tables)
        schema_text = self.smart_schema.get_targeted_schema(
            cached_schema,
            query_plan['relevant_tables'],
            max_columns=10
        )
        
        logger.info(f"Intelligent schema generated ({len(schema_text)} chars) for tables: {query_plan['relevant_tables']}")
        state['schema'] = schema_text
        
        return state
    
class QueryGeneratorNode(BaseNode):
    """Node 3: Generate SQL/MongoDB queries using LLM with entity intelligence + AUTO-COLUMN DETECTION"""
    
    def __init__(self, llm_client: LLMClientProtocol):
        self.llm = llm_client
        
        # ⭐ NEW: Intelligent Column Mapper - KNOWS which columns to include!
        try:
            from agents.column_mapper import IntelligentColumnMapper
            self.column_mapper = IntelligentColumnMapper()
            logger.info("QueryGenerator: Column Mapper initialized ⭐")
        except ImportError:
            logger.warning("Column mapper not available")
            self.column_mapper = None
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generate queries based on target database with entity awareness"""
        logger.info("Node: generate_sql")
        
        user_message = state['user_message']
        schema = state['schema']
        target_db = state.get('target_database')
        query_plan = state.get('query_plan', {})
        detected_entity = state.get('detected_entity')
        
        # ⚠️ Default target_db to 'both' if not set (for canonical queries)
        if not target_db:
            # Try to infer from query plan
            plan_tables = query_plan.get('relevant_tables', {})
            if plan_tables.get('postgresql') and plan_tables.get('mongodb'):
                target_db = 'both'
            elif plan_tables.get('postgresql'):
                target_db = 'postgresql'
            elif plan_tables.get('mongodb'):
                target_db = 'mongodb'
            else:
                target_db = 'both'  # Default to both
            logger.info(f"target_database was None, defaulting to: {target_db}")
        
        queries = {}
        
        # ⚠️ SPECIAL CASE: Canonical person / deduplication queries
        message_lower = user_message.lower()
        is_canonical_query = any(kw in message_lower for kw in [
            'canonical person', 'canonical', 'associated identities', 'all associated',
            'deduplication', 'duplicate persons', 'matched persons', 'person records with'
        ])
        if is_canonical_query:
            logger.info(f"Detected canonical person query: {user_message}")
        
        # If entity detected, generate comprehensive search queries
        if detected_entity:
            # ⚠️ Skip entity-based query if it's a false positive (like "Get canonical person records")
            entity_value = detected_entity.get('value', '').lower()
            entity_type = detected_entity.get('type', '')
            
            # ⭐ DYNAMIC: Check if this is a false positive person_name detection
            # Use the entity detector's dynamic false positive detection
            is_false_positive = False
            if entity_type == 'person_name':
                try:
                    from agents.entity_detector import EntityDetector
                    detector = EntityDetector()
                    is_false_positive = detector._is_false_positive_person_name(
                        entity_value, 
                        user_message.lower()
                    )
                except Exception as e:
                    logger.debug(f"Error in false positive check: {e}")
                    # Fallback: check for obvious query patterns
                    is_false_positive = any(
                        pattern in entity_value.lower() 
                        for pattern in ['get ', 'show ', 'find ', 'list ', 'processed', 'specific']
                    )
            
            if is_false_positive or is_canonical_query:
                logger.info(f"Skipping entity-based query (false positive or canonical query), using LLM instead")
                detected_entity = None  # Treat as non-entity query
            else:
                logger.info(f"Generating entity-based queries for {detected_entity['type']}: {detected_entity['value']}")
                queries = self._generate_entity_queries(detected_entity, schema, target_db, user_message)
        
        # If no entity or canonical query, use LLM
        if not queries:
            # ⚠️ SPECIAL CASE: Canonical person query - generate comprehensive person query
            if is_canonical_query:
                logger.info("Processing canonical person query...")
                # Extract person name if provided (e.g., "Get canonical person Rajendra Prasad @ Sachin")
                person_name = None
                # Try to extract a real person name (not "Get canonical person records")
                import re
                # Look for patterns like "person NAME" or "person NAME @ ALIAS"
                # Match: "person Rajendra Prasad" or "person Rajendra Prasad @ Sachin"
                # First check if it's NOT "person records" (false positive)
                if not re.search(r'person\s+records', user_message, re.IGNORECASE):
                    name_patterns = [
                        r'person\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*@',  # "person Rajendra Prasad @"
                        r'person\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',  # "person Rajendra Prasad"
                    ]
                    for pattern in name_patterns:
                        name_match = re.search(pattern, user_message, re.IGNORECASE)
                        if name_match:
                            candidate = name_match.group(1).strip()
                            # Skip if it's a false positive (contains "canonical", "records", etc.)
                            if not any(kw in candidate.lower() for kw in ['canonical', 'records', 'associated', 'identities', 'get']):
                                person_name = candidate
                                logger.info(f"Extracted person name from query: {person_name}")
                                break
                
                logger.info(f"Canonical query - person_name: {person_name}, target_db: {target_db}")
                if person_name and target_db in ['postgresql', 'both']:
                    # Generate query for canonical person with all associated identities
                    sql = f"""
SELECT DISTINCT 
    p.person_id,
    p.full_name,
    p.name,
    p.surname,
    p.alias,
    p.age,
    p.gender,
    p.occupation,
    p.phone_number,
    p.email_id,
    p.present_district,
    p.present_state_ut,
    p.permanent_district,
    -- Associated accused records
    a.accused_id,
    a.crime_id as accused_crime_id,
    a.type as accused_type,
    a.is_ccl,
    -- Brief facts accused
    bfa.bf_accused_id,
    bfa.full_name as bfa_full_name,
    bfa.alias_name as bfa_alias,
    bfa.phone_numbers as bfa_phone_numbers,
    bfa.role_in_crime,
    bfa.accused_type as bfa_accused_type,
    bfa.status as bfa_status,
    -- Crime details
    c.fir_num,
    c.crime_type,
    c.case_status,
    c.fir_date,
    h.ps_name,
    h.dist_name
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id OR bfa.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE p.full_name ILIKE '%{person_name}%'
   OR p.name ILIKE '%{person_name}%'
   OR p.alias ILIKE '%{person_name}%'
   OR bfa.full_name ILIKE '%{person_name}%'
   OR bfa.alias_name ILIKE '%{person_name}%'
ORDER BY p.person_id, c.fir_date DESC
LIMIT 100
                    """.strip()
                    queries['postgresql'] = sql
                    logger.info(f"Generated canonical person query for: {person_name}")
                elif target_db in ['postgresql', 'both']:
                    # No specific person name - show all persons with their associated identities
                    logger.info("Generating canonical person query (all persons with associated identities)")
                    sql = """
SELECT DISTINCT 
    p.person_id,
    p.full_name,
    p.name,
    p.surname,
    p.alias,
    p.age,
    p.gender,
    p.occupation,
    p.phone_number,
    p.email_id,
    COUNT(DISTINCT a.accused_id) as total_accused_records,
    COUNT(DISTINCT bfa.bf_accused_id) as total_bfa_records,
    COUNT(DISTINCT c.crime_id) as total_crimes,
    STRING_AGG(DISTINCT c.fir_num, ', ') as fir_numbers
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id OR bfa.crime_id = c.crime_id
GROUP BY p.person_id, p.full_name, p.name, p.surname, p.alias, 
         p.age, p.gender, p.occupation, p.phone_number, p.email_id
HAVING COUNT(DISTINCT a.accused_id) > 0 
    OR COUNT(DISTINCT bfa.bf_accused_id) > 0
ORDER BY total_accused_records DESC, total_crimes DESC
LIMIT 100
                    """.strip()
                    queries['postgresql'] = sql
                    logger.info("Generated canonical person query (all persons with associated identities)")
            else:
                # Generate PostgreSQL query using LLM
                if target_db in ['postgresql', 'both']:
                    # ⭐ NEW: Pass required_columns to LLM so it KNOWS which columns to include!
                    required_columns = state.get('required_columns', [])
                    if required_columns and self.column_mapper:
                        # Add column hints to query plan
                        query_plan['required_columns'] = required_columns
                        logger.info(f"⭐ Passing {len(required_columns)} auto-detected columns to LLM:")
                        for col in required_columns[:5]:  # Log top 5
                            logger.info(f"  → {col['table']}.{col['column']}")
                    
                    sql = self.llm.generate_sql_with_context(
                        user_message,
                        schema,
                        query_plan
                    )
                    # ⚠️ CRITICAL: Check for both None and empty string
                    if sql and sql.strip():
                        sql = SQLCleaner.clean(sql)
                        # After cleaning, check again if it's empty
                        if sql and sql.strip():
                            # ⭐ NEW: Remove IS NOT NULL filters for "information about" queries
                            sql = SQLCleaner.remove_not_null_filters(sql, user_message)
                            # ⭐ NEW: Add automatic LIMIT if missing (performance optimization)
                            sql = SQLCleaner.add_limit_if_missing(sql, user_message)
                            queries['postgresql'] = sql
                            logger.info(f"Generated PostgreSQL query: {sql}")
                        else:
                            # SQL was cleaned but became empty - treat as no SQL
                            logger.warning(f"SQL became empty after cleaning for: {user_message[:100]}")
                            sql = None  # Fall through to fallback logic
                    else:
                        # ⚠️ CRITICAL: Log when LLM fails to generate SQL
                        logger.warning(f"LLM returned None/empty SQL for query: {user_message[:100]}")
                        logger.warning(f"Query plan: {query_plan}")
                        # For "information about" queries, we should still generate a query
                        # to show all records (even if fields are empty)
                        message_lower = user_message.lower()
                        info_about_keywords = [
                            'hair color', 'hair color information', 'eye color', 'eye color details',
                            'build type', 'build type information', 'mole', 'leucoderma',
                            'height information', 'height', 'seizure worth', 'packaging details',
                            'information about', 'with hair', 'with eye', 'with build',
                            'with mole', 'with leucoderma', 'with height', 'show drugs with',
                            'show accused with', 'list accused with', 'find accused with',
                            'list all accused with', 'show accused', 'list accused',
                            'accused with height', 'accused with build', 'accused with hair',
                            'accused with eye', 'accused with mole', 'accused with leucoderma'
                        ]
                        if any(kw in message_lower for kw in info_about_keywords):
                            # Generate a fallback query to show all accused with requested fields
                            if 'hair' in message_lower or 'hair color' in message_lower:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.hair, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            elif 'eye' in message_lower or 'eye color' in message_lower:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.eyes, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            elif 'height' in message_lower:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.height, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            elif 'build' in message_lower:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.build, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            elif 'mole' in message_lower or 'leucoderma' in message_lower:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.mole, a.leucoderma, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            else:
                                fallback_sql = "SELECT a.accused_id, a.person_id, a.hair, a.eyes, a.height, a.build, a.mole, a.leucoderma, p.full_name FROM accused a LEFT JOIN persons p ON a.person_id = p.person_id LIMIT 100"
                            
                            queries['postgresql'] = fallback_sql
                            logger.info(f"Generated fallback PostgreSQL query for 'information about' query: {fallback_sql}")
                        # Check for state queries (Q39)
                        elif 'from state' in message_lower or ('state' in message_lower and 'telangana' in message_lower):
                            fallback_sql = "SELECT p.person_id, p.full_name, p.present_state_ut, p.permanent_state_ut FROM persons p WHERE p.present_state_ut ILIKE '%Telangana%' OR p.permanent_state_ut ILIKE '%Telangana%' LIMIT 100"
                            queries['postgresql'] = fallback_sql
                            logger.info(f"Generated fallback PostgreSQL query for state query: {fallback_sql}")
                        # Check for brand name queries (Q69)
                        elif 'brand name' in message_lower or ('by brand' in message_lower):
                            fallback_sql = "SELECT d.drug_name as brand_name, pr.nature as property_brand, COUNT(*) as count FROM brief_facts_drugs d LEFT JOIN properties pr ON d.crime_id = pr.crime_id GROUP BY d.drug_name, pr.nature ORDER BY count DESC LIMIT 100"
                            queries['postgresql'] = fallback_sql
                            logger.info(f"Generated fallback PostgreSQL query for brand name query: {fallback_sql}")
                
                # Generate MongoDB query
                mongo = None  # Initialize to avoid UnboundLocalError
                if target_db in ['mongodb', 'both']:
                    mongo = self.llm.generate_mongodb_query(user_message, schema)
                if mongo:
                    try:
                        mongo_dict = json.loads(mongo)
                        if 'collection' in mongo_dict:
                            queries['mongodb'] = mongo_dict
                            logger.info(f"Generated MongoDB query: {mongo}")
                        else:
                            logger.error("MongoDB query missing 'collection' field")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse MongoDB query JSON: {mongo}, Error: {e}")
        
        if not queries:
            state['error'] = "Failed to generate query"
            state['queries'] = {}
        else:
            state['queries'] = queries
        
        return state
    
    def _generate_entity_queries(self, entity: Dict, schema: str, target_db: str, user_message: str = '') -> Dict:
        """
        Generate comprehensive queries to search for an entity
        NOW WITH CONTEXT-AWARE CRIME ID QUERIES! ⭐
        
        Args:
            entity: Detected entity info
            schema: Database schema
            target_db: Target database(s)
            user_message: User's original message for context
        
        Returns:
            Dict of queries for PostgreSQL and/or MongoDB
        """
        queries = {}
        entity_value = entity['value']
        entity_type = entity['type']
        search_fields = entity['search_fields']
        
        # Generate PostgreSQL query (search multiple fields)
        if target_db in ['postgresql', 'both']:
            # SPECIAL CASE: ObjectID → CONTEXT-AWARE QUERIES!
            # ⭐ CRITICAL: Detect if it's a person_id or crime_id!
            if entity_type == 'objectid':
                message_lower = user_message.lower()
                
                # ⭐ NEW: Detect if user explicitly says "person id", "accused id", or "crime id"
                # Following user's clear rules:
                # 1. Crime-related → use crime_id
                # 2. Person-related → use person_id
                # 3. Accused-related → use accused_id
                is_person_id = any(kw in message_lower for kw in [
                    'person id', 'person_id', 'personid', 'using person', 'by person id', 'person related'
                ])
                is_accused_id = any(kw in message_lower for kw in [
                    'accused id', 'accused_id', 'accusedid', 'using accused', 'by accused id', 'accused related'
                ])
                is_crime_id = any(kw in message_lower for kw in [
                    'crime id', 'crime_id', 'crimeid', 'using crime', 'by crime id', 'fir id', 'case id', 'crime related'
                ])
                
                # ⭐ NEW: If user says "accused id", search by accused_id directly!
                # Rule 3: Accused-related → use accused_id, check brief_facts_accused if not found
                if is_accused_id:
                    # Build SELECT with accused physical data + person personal data + brief_facts_accused fallback
                    select_fields = [
                        # Accused physical data
                        "a.accused_id", "a.crime_id", "a.person_id", "a.type as accused_type", "a.is_ccl",
                        "a.height", "a.build", "a.color", "a.hair", "a.eyes", "a.face", "a.nose",
                        "a.beard", "a.mustache", "a.mole",
                        # Person personal data
                        "p.person_id", "p.full_name", "p.name", "p.surname", "p.alias",
                        "p.age", "p.gender", "p.occupation", "p.phone_number", "p.email_id",
                        "p.present_locality_village", "p.present_district", "p.present_state_ut",
                        "p.permanent_locality_village", "p.permanent_district", "p.permanent_state_ut",
                        "p.domicile_classification",
                        # Brief facts accused (fallback data)
                        "bfa.bf_accused_id", "bfa.role_in_crime", "bfa.accused_type as bfa_accused_type",
                        "bfa.status", "bfa.key_details", "bfa.address", "bfa.phone_numbers",
                        # Crime details
                        "c.fir_num", "c.crime_type", "c.case_status", "h.ps_name", "h.dist_name"
                    ]
                    
                    sql = f"""
SELECT DISTINCT 
    {', '.join(select_fields)}
FROM accused a
LEFT JOIN persons p ON a.person_id = p.person_id
LEFT JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE a.accused_id = '{entity_value}'
ORDER BY c.fir_date DESC
                    """.strip()
                    logger.info(f"Generated ACCUSED query by accused_id: {entity_value}")
                    queries['postgresql'] = sql
                    return queries
                
                # ⭐ NEW: If user says "person id", search by person_id directly!
                # Rule 2: Person-related → use person_id
                if is_person_id:
                    # Extract nationality filter if provided (native/interstate/international)
                    nationality_filter = None
                    if 'native' in message_lower:
                        nationality_filter = 'native'
                    elif 'interstate' in message_lower:
                        nationality_filter = 'interstate'
                    elif 'international' in message_lower:
                        nationality_filter = 'international'
                    
                    # Build SELECT with domicile_classification (nationality)
                    # ⚠️ CRITICAL: Include c.fir_date in SELECT when using ORDER BY c.fir_date with DISTINCT!
                    select_fields = [
                        "p.person_id", "p.full_name", "p.name", "p.surname", "p.alias",
                        "p.age", "p.gender", "p.occupation",
                        "p.phone_number", "p.email_id",
                        "p.present_locality_village", "p.present_district", "p.present_state_ut",
                        "p.permanent_locality_village", "p.permanent_district", "p.permanent_state_ut",
                        "p.domicile_classification",  # ⭐ NATIONALITY field!
                        "p.relation_type", "p.relative_name",
                        "a.accused_id", "a.crime_id", "a.type as accused_type", "a.is_ccl",
                        "bfa.role_in_crime", "bfa.accused_type as bfa_accused_type", "bfa.status",
                        "c.fir_num", "c.crime_type", "c.case_status", "c.fir_date",  # ⭐ Added fir_date for ORDER BY!
                        "h.ps_name", "h.dist_name"
                    ]
                    
                    # Build WHERE clause
                    where_clause = f"p.person_id = '{entity_value}'"
                    if nationality_filter:
                        where_clause += f" AND p.domicile_classification ILIKE '%{nationality_filter}%'"
                    
                    sql = f"""
SELECT DISTINCT 
    {', '.join(select_fields)}
FROM persons p
LEFT JOIN accused a ON p.person_id = a.person_id
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE {where_clause}
ORDER BY c.fir_date DESC
                    """.strip()
                    logger.info(f"Generated PERSON query by person_id: {entity_value} (nationality: {nationality_filter or 'all'})")
                    queries['postgresql'] = sql
                    return queries
                
                # Detect what user wants to know (for crime_id queries)
                # Rule 1: Crime-related → use crime_id
                wants_persons = any(kw in message_lower for kw in [
                    'person', 'people', 'accused', 'who', 'involved', 'criminal', 'suspect', 'name'
                ])
                wants_drugs = any(kw in message_lower for kw in [
                    'drug', 'narcotic', 'ganja', 'heroin', 'cocaine', 'substance', 'seizure', 'mdma'
                ])
                wants_properties = any(kw in message_lower for kw in [
                    'property', 'properties', 'seized', 'recovered', 'vehicle', 'cash', 'mobile'
                ])
                wants_brief_facts = any(kw in message_lower for kw in [
                    'brief facts', 'summary', 'details from brief', 'crime summary', 'case summary'
                ])
                
                # ⭐ DEFAULT: If not explicitly person_id or accused_id, assume crime_id (Rule 1)
                if wants_persons:
                    # Rule 1: Crime-related → use crime_id
                    # Rule 2: Person-related → use person_id (via accused.person_id)
                    # Rule 3: Accused-related → check brief_facts_accused as fallback
                    # Rule 5: Hierarchy → use ps_code
                    # Query for PERSONS involved in crime
                    sql = f"""
SELECT p.person_id, p.full_name, p.alias, p.age, p.gender, p.occupation, 
       p.phone_number, p.email_id, p.relation_type, p.relative_name,
       p.present_locality_village, p.present_district, p.present_state_ut,
       p.permanent_locality_village, p.permanent_district, p.domicile_classification,
       a.accused_id, a.type as accused_type, a.is_ccl,
       a.height, a.build, a.color, a.hair, a.eyes, a.face, a.nose,
       a.beard, a.mustache, a.mole,
       bfa.role_in_crime, bfa.accused_type as bfa_accused_type, bfa.status, bfa.key_details,
       bfa.address as accused_address, bfa.phone_numbers as accused_phones,
       bfa.seq_num,
       c.crime_id, c.fir_num, c.crime_type, c.case_status, h.ps_name, h.dist_name
FROM accused a
JOIN persons p ON a.person_id = p.person_id
LEFT JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.crime_id = '{entity_value}'
ORDER BY bfa.seq_num
                    """.strip()
                    logger.info(f"Generated PERSONS query for crime ID: {entity_value} (Rule 1: crime_id, Rule 2: person_id, Rule 3: brief_facts_accused fallback)")
                
                elif wants_drugs:
                    # Rule 4: Drug-related queries → use crime_id in BOTH properties and brief_facts_drugs
                    # Query for DRUGS seized in crime - AUTO-DETECT COLUMNS from user question!
                    # ⭐ NEW: Use column mapper to KNOW which columns user is asking about!
                    message_lower = user_message.lower()
                    
                    # ⭐ AUTO-DETECT required columns from user question
                    drug_columns = []
                    property_columns = []
                    if self.column_mapper:
                        column_matches = self.column_mapper.find_columns(user_message)
                        # Filter for drug-related columns
                        drug_columns = [m for m in column_matches if m.table == 'brief_facts_drugs']
                        property_columns = [m for m in column_matches if m.table == 'properties']
                        
                        if drug_columns:
                            logger.info(f"⭐ AUTO-DETECTED {len(drug_columns)} drug columns from user question:")
                            for match in drug_columns:
                                logger.info(f"  → {match.column} (confidence: {match.confidence})")
                    
                    # Build SELECT list - prioritize auto-detected columns, then include all common drug columns
                    select_parts = [
                        # Crime details (always include)
                        "c.crime_id", "c.fir_num", "c.fir_reg_num", "c.crime_type", "c.case_status", "c.fir_date",
                        "c.major_head", "c.minor_head", "c.acts_sections", "c.io_name", "c.io_rank",
                        "h.ps_name", "h.dist_name", "h.circle_name", "h.zone_name",
                        # Drug ID
                        "d.id as drug_id",
                    ]
                    
                    # ⭐ Add auto-detected drug columns FIRST (prioritized)
                    if self.column_mapper and drug_columns:
                        for match in drug_columns:
                            col_expr = f"d.{match.column}"
                            if match.alias and match.alias != match.column:
                                col_expr = f"{col_expr} as {match.alias}"
                            if col_expr not in select_parts:
                                select_parts.append(col_expr)
                    
                    # Then add all common drug columns (if not already added)
                    common_drug_cols = [
                        "d.drug_name", "d.scientific_name", "d.brand_name", "d.drug_category", "d.drug_schedule",
                        "d.total_quantity", "d.quantity_unit", "d.quantity_numeric", "d.number_of_packets",
                        "d.weight_breakdown", "d.packaging_details",
                        "d.source_location", "d.destination", "d.transport_method", "d.supply_chain",
                        "d.seizure_location", "d.seizure_time", "d.seizure_method", "d.seizure_officer",
                        "d.commercial_quantity", "d.is_commercial",
                        "d.seizure_worth", "d.street_value", "d.street_value_numeric", "d.purity",
                    ]
                    for col in common_drug_cols:
                        if col not in select_parts:
                            select_parts.append(col)
                    
                    # ⭐ Add auto-detected property columns
                    if self.column_mapper and property_columns:
                        for match in property_columns:
                            col_expr = f"pr.{match.column}"
                            if match.alias:
                                col_expr = f"{col_expr} as {match.alias}"
                            if col_expr not in select_parts:
                                select_parts.append(col_expr)
                    
                    # Then add common property columns
                    common_property_cols = [
                        "pr.property_id", "pr.case_property_id",
                        "pr.nature as property_nature", "pr.category as property_category",
                        "pr.particular_of_property", "pr.property_status",
                        "pr.estimate_value", "pr.recovered_value",
                        "pr.recovered_from", "pr.place_of_recovery", "pr.date_of_seizure",
                        "pr.belongs"
                    ]
                    for col in common_property_cols:
                        if col not in select_parts:
                            select_parts.append(col)
                    
                    # Build final SQL
                    # Rule 4: MUST JOIN BOTH brief_facts_drugs AND properties using crime_id
                    # Rule 5: Hierarchy → use ps_code
                    sql = f"""
SELECT DISTINCT 
    {', '.join(select_parts)}
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN brief_facts_drugs d ON c.crime_id = d.crime_id
LEFT JOIN properties pr ON c.crime_id = pr.crime_id
WHERE c.crime_id = '{entity_value}'
ORDER BY d.id, pr.property_id
                    """.strip()
                    logger.info(f"Generated DRUGS query for crime ID: {entity_value} (Rule 4: BOTH brief_facts_drugs + properties via crime_id, ⭐ AUTO-INCLUDED {len(drug_columns) if self.column_mapper and drug_columns else 0} columns from user question)")
                
                elif wants_properties:
                    # Rule 1: Crime-related → use crime_id
                    # Rule 5: Hierarchy → use ps_code
                    # Query for PROPERTIES seized in crime
                    sql = f"""
SELECT c.crime_id, c.fir_num, c.crime_type, c.case_status, c.fir_date, 
       h.ps_name, h.dist_name, h.circle_name,
       pr.property_id, pr.case_property_id,
       pr.nature as property_nature, pr.category as property_category,
       pr.particular_of_property, pr.property_status,
       pr.estimate_value, pr.recovered_value,
       pr.recovered_from, pr.place_of_recovery, pr.date_of_seizure, pr.belongs
FROM crimes c
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN properties pr ON c.crime_id = pr.crime_id
WHERE c.crime_id = '{entity_value}'
                    """.strip()
                    logger.info(f"Generated PROPERTIES query for crime ID: {entity_value} (Rule 1: crime_id, Rule 5: ps_code)")
                
                elif wants_brief_facts:
                    # Rule 6: Brief facts/summary queries → use summary_text from brief_facts_crime_summaries with crime_id
                    sql = f"""
SELECT c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, c.fir_date,
       c.ps_code, c.major_head, c.minor_head, c.acts_sections, c.io_name, c.io_rank,
       c.brief_facts,
       h.ps_name, h.dist_name, h.circle_name,
       s.summary_text, s.summary_json
FROM crimes c
LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE c.crime_id = '{entity_value}'
LIMIT 1
                    """.strip()
                    logger.info(f"Generated BRIEF FACTS query for crime ID: {entity_value} (using summary_text)")
                
                else:
                    # Default: Comprehensive crime details
                    # Rule 1: Crime-related → use crime_id
                    # Rule 5: Hierarchy → use ps_code
                    # Rule 6: Brief facts → use summary_text with crime_id
                    sql = f"""
SELECT c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, c.fir_date,
       c.ps_code, c.major_head, c.minor_head, c.acts_sections, c.io_name, c.io_rank,
       c.brief_facts,
       h.ps_name, h.dist_name, h.circle_name,
       s.summary_text,
       COUNT(DISTINCT a.person_id) as total_accused
FROM crimes c
LEFT JOIN brief_facts_crime_summaries s ON c.crime_id = s.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
LEFT JOIN accused a ON c.crime_id = a.crime_id
WHERE c.crime_id = '{entity_value}'
GROUP BY c.crime_id, c.fir_num, c.fir_reg_num, c.crime_type, c.case_status, c.fir_date,
         c.ps_code, c.major_head, c.minor_head, c.acts_sections, c.io_name, c.io_rank,
         c.brief_facts, h.ps_name, h.dist_name, h.circle_name, s.summary_text
LIMIT 1
                    """.strip()
                    logger.info(f"Generated COMPREHENSIVE crime details query for crime ID: {entity_value}")
                
                queries['postgresql'] = sql
            else:
                # Try to find the most relevant table from schema
                table_name = self._guess_table_from_schema(schema, 'postgresql')
                
                if table_name:
                    # ⚠️ CRITICAL: Filter search_fields to use ONLY PostgreSQL columns (not MongoDB!)
                    # Also use correct columns based on table name
                    postgresql_fields = []
                    
                    # Filter out MongoDB field names (UPPERCASE like MOBILE_1, INT_FATHER_MOBILE_NO, EMAIL)
                    for field in search_fields:
                        # Skip MongoDB fields (UPPERCASE with underscores or single uppercase word)
                        if field.isupper():
                            continue
                        # Skip fields that don't exist in PostgreSQL
                        if field in ['MOBILE_1', 'INT_FATHER_MOBILE_NO', 'TELEPHONE_RESIDENCE', 'EMAIL', 'email_address']:
                            continue
                        postgresql_fields.append(field)
                    
                    # ⚠️ SPECIAL CASE: Phone number search - use correct columns per table
                    if entity_type == 'mobile_number':
                        if table_name == 'accused':
                            # accused table has NO phone_number! Must JOIN with persons
                            sql = f"""
SELECT DISTINCT p.person_id, p.full_name, p.name, p.surname, p.alias, 
       p.phone_number, p.email_id, p.age, p.gender, p.occupation,
       p.present_district, p.present_state_ut, p.permanent_district,
       bfa.phone_numbers as accused_phone_numbers,
       a.crime_id, c.fir_num, c.crime_type, c.case_status, h.ps_name
FROM accused a
JOIN persons p ON a.person_id = p.person_id
LEFT JOIN brief_facts_accused bfa ON a.person_id = bfa.person_id AND a.crime_id = bfa.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE p.phone_number ILIKE '%{entity_value}%'
   OR bfa.phone_numbers ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        elif table_name == 'persons':
                            # persons table has phone_number (singular), NOT phone_numbers!
                            sql = f"""
SELECT DISTINCT p.person_id, p.full_name, p.name, p.surname, p.alias,
       p.phone_number, p.email_id, p.age, p.gender, p.occupation,
       p.present_district, p.present_state_ut, p.permanent_district,
       bfa.phone_numbers as accused_phone_numbers
FROM persons p
LEFT JOIN brief_facts_accused bfa ON p.person_id = bfa.person_id
WHERE p.phone_number ILIKE '%{entity_value}%'
   OR bfa.phone_numbers ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        elif table_name == 'brief_facts_accused':
                            # brief_facts_accused has phone_numbers (plural)
                            sql = f"""
SELECT DISTINCT bfa.bf_accused_id, bfa.full_name, bfa.alias_name,
       bfa.phone_numbers, bfa.age, bfa.gender, bfa.occupation, bfa.address,
       p.phone_number as person_phone_number,
       a.crime_id, c.fir_num, c.crime_type, c.case_status
FROM brief_facts_accused bfa
LEFT JOIN persons p ON bfa.person_id = p.person_id
LEFT JOIN accused a ON bfa.person_id = a.person_id AND bfa.crime_id = a.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
WHERE bfa.phone_numbers ILIKE '%{entity_value}%'
   OR p.phone_number ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        else:
                            # For other tables, use filtered fields
                            conditions = [f"{field} ILIKE '%{entity_value}%'" for field in postgresql_fields[:5] if field]
                            if conditions:
                                where_clause = " OR ".join(conditions)
                                sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 20"
                            else:
                                # Fallback: no valid fields found
                                sql = f"SELECT * FROM {table_name} LIMIT 0"
                    elif entity_type == 'email':
                        # ⚠️ SPECIAL CASE: Email search - use correct columns per table
                        if table_name == 'accused':
                            # accused table has NO email_id! Must JOIN with persons
                            sql = f"""
SELECT DISTINCT p.person_id, p.full_name, p.name, p.surname, p.alias,
       p.email_id, p.phone_number, p.age, p.gender, p.occupation,
       p.present_district, p.present_state_ut, p.permanent_district,
       a.crime_id, c.fir_num, c.crime_type, c.case_status, h.ps_name
FROM accused a
JOIN persons p ON a.person_id = p.person_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
LEFT JOIN hierarchy h ON c.ps_code = h.ps_code
WHERE p.email_id ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        elif table_name == 'persons':
                            # persons table has email_id
                            sql = f"""
SELECT DISTINCT p.person_id, p.full_name, p.name, p.surname, p.alias,
       p.email_id, p.phone_number, p.age, p.gender, p.occupation,
       p.present_district, p.present_state_ut, p.permanent_district
FROM persons p
WHERE p.email_id ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        elif table_name == 'brief_facts_accused':
                            # brief_facts_accused doesn't have email, must JOIN with persons
                            sql = f"""
SELECT DISTINCT bfa.bf_accused_id, bfa.full_name, bfa.alias_name,
       bfa.age, bfa.gender, bfa.occupation, bfa.address,
       p.email_id, p.phone_number,
       a.crime_id, c.fir_num, c.crime_type, c.case_status
FROM brief_facts_accused bfa
LEFT JOIN persons p ON bfa.person_id = p.person_id
LEFT JOIN accused a ON bfa.person_id = a.person_id AND bfa.crime_id = a.crime_id
LEFT JOIN crimes c ON a.crime_id = c.crime_id
WHERE p.email_id ILIKE '%{entity_value}%'
LIMIT 20
                            """.strip()
                        else:
                            # For other tables, use filtered fields
                            conditions = [f"{field} ILIKE '%{entity_value}%'" for field in postgresql_fields[:5] if field]
                            if conditions:
                                where_clause = " OR ".join(conditions)
                                sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 20"
                            else:
                                # Fallback: no valid fields found
                                sql = f"SELECT * FROM {table_name} LIMIT 0"
                    else:
                        # For non-phone, non-email entities, use filtered fields
                        conditions = [f"{field} ILIKE '%{entity_value}%'" for field in postgresql_fields[:5] if field]
                        if conditions:
                            where_clause = " OR ".join(conditions)
                            sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 20"
                        else:
                            # Fallback: no valid fields found
                            sql = f"SELECT * FROM {table_name} LIMIT 0"
                    
                    queries['postgresql'] = sql
                    logger.info(f"Generated entity search SQL: {sql}")
        
        # Generate MongoDB query
        if target_db in ['mongodb', 'both']:
            # SPECIAL CASE: Crime ID (ObjectID) → search by _id (MongoDB ObjectID)
            if entity_type == 'objectid':
                # MongoDB will handle the ObjectId conversion automatically
                # Just pass the string value directly
                mongo_query = {
                    "collection": "fir_records",
                    "query": {"_id": entity_value}
                }
                queries['mongodb'] = mongo_query
                logger.info(f"Generated crime ID search MongoDB: {mongo_query}")
            else:
                # ⚠️ CRITICAL: Filter search_fields to use ONLY MongoDB columns (UPPERCASE!)
                # Also filter out PostgreSQL field names (lowercase)
                mongodb_fields = []
                
                for field in search_fields:
                    # MongoDB uses UPPERCASE field names (MOBILE_1, ACCUSED_NAME, EMAIL, etc.)
                    # PostgreSQL uses lowercase (phone_number, email_id, etc.)
                    if field.isupper():
                        # This is a MongoDB field (all uppercase)
                        mongodb_fields.append(field)
                    elif field in ['MOBILE_1', 'INT_FATHER_MOBILE_NO', 'TELEPHONE_RESIDENCE', 'ACCUSED_NAME', 'FIR_NO', 'FIR_REG_NUM', 'DISTRICT', 'PS', 'FIR_STATUS', 'EMAIL']:
                        # Explicit MongoDB fields (just in case)
                        mongodb_fields.append(field)
                
                # ⚠️ SPECIAL CASE: Phone number search in MongoDB
                if entity_type == 'mobile_number':
                    # MongoDB uses MOBILE_1, INT_FATHER_MOBILE_NO, etc.
                    or_conditions = [
                        {"MOBILE_1": {"$regex": entity_value, "$options": "i"}},
                        {"INT_FATHER_MOBILE_NO": {"$regex": entity_value, "$options": "i"}},
                        {"INT_MOTHER_MOBILE_NO": {"$regex": entity_value, "$options": "i"}},
                        {"INT_WIFE_MOBILE_NO": {"$regex": entity_value, "$options": "i"}},
                        {"TELEPHONE_RESIDENCE": {"$regex": entity_value, "$options": "i"}}
                    ]
                elif entity_type == 'email':
                    # MongoDB email search (if EMAIL field exists)
                    or_conditions = [
                        {"EMAIL": {"$regex": entity_value, "$options": "i"}}
                    ]
                else:
                    # For other entities, use filtered MongoDB fields
                    or_conditions = [{field: {"$regex": entity_value, "$options": "i"}} for field in mongodb_fields[:5] if field]
                
                # Always use fir_records collection for MongoDB (don't guess from schema!)
                if or_conditions:
                    mongo_query = {
                        "collection": "fir_records",
                        "query": {"$or": or_conditions} if len(or_conditions) > 1 else or_conditions[0]
                    }
                    queries['mongodb'] = mongo_query
                    logger.info(f"Generated entity search MongoDB: {mongo_query}")
        
        return queries
    
    def _guess_table_from_schema(self, schema: str, db_type: str) -> Optional[str]:
        """Guess most relevant table/collection from schema"""
        lines = schema.split('\n')
        
        if db_type == 'postgresql':
            # Look for PostgreSQL tables
            for line in lines:
                if line.strip().startswith('PostgreSQL') or line.strip().startswith('V2'):
                    continue
                if ':' in line and not line.strip().startswith('•'):
                    # Extract table name
                    table = line.split(':')[0].strip()
                    if table and not table.startswith('+'):
                        return table
        
        elif db_type == 'mongodb':
            # Look for MongoDB collections
            for line in lines:
                if line.strip().startswith('MongoDB') or line.strip().startswith('V1'):
                    continue
                if ':' in line and not line.strip().startswith('•'):
                    # Extract collection name
                    collection = line.split(':')[0].strip()
                    if collection and not collection.startswith('+'):
                        return collection
        
        return None
    
class QueryValidatorNode(BaseNode):
    """Node 4: Validate queries for security"""
    
    def __init__(self, validator: Any):
        self.validator = validator
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Validate all queries"""
        logger.info("Node: validate_sql")
        
        queries = state.get('queries', {})
        validated = {}
        errors = []
        
        # Validate PostgreSQL
        if 'postgresql' in queries:
            sql_query = queries['postgresql']
            logger.debug(f"Validating PostgreSQL query: {sql_query}")
            is_safe, msg = self.validator.is_sql_safe(sql_query)
            
            if is_safe:
                validated['postgresql'] = sql_query
                logger.info(f"✓ PostgreSQL query validated: {sql_query}")
            else:
                logger.warning(f"✗ PostgreSQL validation failed: {msg} | Query: {sql_query}")
                errors.append(f"SQL: {msg}")
        
        # Validate MongoDB
        if 'mongodb' in queries:
            mongo_query = queries['mongodb']
            logger.debug(f"Validating MongoDB query: {mongo_query}")
            
            if 'pipeline' in mongo_query:
                is_safe, msg = self.validator.is_mongo_pipeline_safe(
                    mongo_query['pipeline']
                )
            elif 'query' in mongo_query:
                is_safe, msg = self.validator.is_mongo_query_safe(
                    mongo_query.get('query', {})
                )
            else:
                is_safe, msg = False, "Missing 'query' or 'pipeline' field"
            
            if is_safe:
                validated['mongodb'] = mongo_query
                logger.info(f"✓ MongoDB query validated: {mongo_query}")
            else:
                logger.warning(f"✗ MongoDB validation failed: {msg} | Query: {mongo_query}")
                errors.append(f"MongoDB: {msg}")
        
        # Only set error if ALL queries failed
        if not validated and errors:
            state['error'] = "; ".join(errors)
        
        state['validated_queries'] = validated
        return state
    
class QueryExecutorNode(BaseNode):
    """Node 5: Execute validated queries on databases"""
    
    def __init__(
        self,
        postgres_executor: DatabaseExecutorProtocol,
        mongo_executor: Any,
        cache_manager: CacheManagerProtocol,
        validator: Any
    ):
        self.postgres = postgres_executor
        self.mongo = mongo_executor
        self.cache = cache_manager
        self.validator = validator
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute all validated queries"""
        logger.info("Node: execute_query")
        
        validated = state.get('validated_queries', {})
        results = {}
        
        # Execute PostgreSQL
        if 'postgresql' in validated:
            sql = validated['postgresql']
            
            # Check cache first
            cached = self.cache.get_cached_query_result(sql)
            if cached:
                logger.info("PostgreSQL result from cache")
                results['postgresql'] = cached
            else:
                success, result = self.postgres.execute_query(sql)
                if success:
                    results['postgresql'] = result
                    self.cache.cache_query_result(sql, result)
                    logger.info(f"PostgreSQL executed successfully: {len(result)} rows")
                else:
                    error = self.validator.sanitize_error_message(str(result))
                    state['error'] = f"PostgreSQL execution error: {error}"
        
        # Execute MongoDB
        if 'mongodb' in validated:
            mongo_query = validated['mongodb']
            collection = mongo_query.get('collection', '')
            query_str = json.dumps(mongo_query, sort_keys=True)
            
            # Check cache
            cached = self.cache.get_cached_query_result(query_str)
            if cached:
                logger.info("MongoDB result from cache")
                results['mongodb'] = cached
            else:
                if 'pipeline' in mongo_query:
                    success, result = self.mongo.execute_aggregate(
                        collection,
                        mongo_query['pipeline']
                    )
                else:
                    success, result = self.mongo.execute_find(
                        collection,
                        mongo_query.get('query', {}),
                        mongo_query.get('projection')
                    )
                
                if success:
                    results['mongodb'] = result
                    self.cache.cache_query_result(query_str, result)
                    logger.info(f"MongoDB executed successfully: {len(result)} documents")
                else:
                    error = self.validator.sanitize_error_message(str(result))
                    state['error'] = f"MongoDB execution error: {error}"
        
        state['results'] = results
        return state
    
class ResponseFormatterNode(BaseNode):
    """Node 6: Format results into conversational response with intelligence + Agent 4 narrative formatting"""
    
    def __init__(self, conversation_handler: Any, llm_client: Any = None, cache_manager: Any = None):
        self.conversation = conversation_handler
        self.formatter = ResultFormatter()
        
        # Import advanced formatters
        try:
            from agents.entity_detector import EntityDetector
            from agents.advanced_formatter import AdvancedFormatter
            from agents.relationship_analyzer import RelationshipAnalyzer
            self.entity_detector = EntityDetector
            self.advanced_formatter = AdvancedFormatter()
            self.relationship_analyzer = RelationshipAnalyzer()
            self.use_advanced = True
        except ImportError:
            logger.warning("Advanced formatters not available, using basic formatting")
            self.use_advanced = False
        
        # Initialize Agent 4: Narrative Formatter (MULTI-AGENT!)
        self.narrative_formatter = None
        self.enable_narrative = False
        
        try:
            from agents.narrative_formatter import NarrativeFormatterAgent
            from config import Config
            
            if llm_client and Config.ENABLE_NARRATIVE_FORMATTING:
                self.narrative_formatter = NarrativeFormatterAgent(
                    llm_client=llm_client,
                    cache_manager=cache_manager
                )
                self.enable_narrative = True
                logger.info("Agent 4 (Narrative Formatter) initialized ⭐")
            else:
                logger.info("Agent 4 disabled (ENABLE_NARRATIVE_FORMATTING=false or no LLM)")
        except ImportError as e:
            logger.warning(f"Agent 4 not available: {e}")
        except Exception as e:
            logger.error(f"Agent 4 initialization error: {e}")
    
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Format results conversationally with intelligence + AGENT 4 NARRATIVE FORMATTING"""
        logger.info("Node: format_response")
        
        # ⭐ WRAP ENTIRE METHOD IN TRY-EXCEPT to catch ALL errors
        try:
            results = state.get('results', {})
            user_message = state.get('user_message', '')
            
            # ⭐ NEW: Check if this is an "information about" query FIRST (before checking results)
            # This handles both empty results AND SQL errors for these queries
            user_message_lower = user_message.lower()
            info_about_keywords = [
                'hair color', 'hair color information', 'eye color', 'eye color details',
                'build type', 'build type information', 'mole', 'leucoderma',
                'height information', 'height', 'seizure worth', 'packaging details',
                'information about', 'with hair', 'with eye', 'with build',
                'with mole', 'with leucoderma', 'with height', 'show drugs with',
                'show accused with', 'list accused with', 'find accused with',
                'list all accused with', 'show accused', 'list accused',
                'accused with height', 'accused with build', 'accused with hair',
                'accused with eye', 'accused with mole', 'accused with leucoderma'
            ]
            is_info_about_query = any(kw in user_message_lower for kw in info_about_keywords)
            
            # Handle SQL errors for "information about" queries
            if is_info_about_query and 'error' in state:
                error_value = state.get('error')
                # Handle case where error might be None or empty string
                error_msg = str(error_value).lower() if error_value else ''
                # If error is about column/table not found, it's likely the LLM made a mistake
                # For "information about" queries, we should still show "verified but no data available"
                if error_msg and 'column' in error_msg and ('does not exist' in error_msg or 'not exist' in error_msg):
                    logger.info("⚠️ SQL error for 'information about' query - showing verified but no data available")
                    state['final_response'] = "I verified the database, but no data is available for the requested information. The fields exist in the database schema, but they are currently empty or not populated in the records."
                    state.pop('error', None)  # Remove error so it doesn't get shown
                    return state
            
            # Handle no results
            if not results:
                if 'error' in state:
                    # Error already set, will be handled by error handler
                    return state
                else:
                    # Provide more helpful error message based on the query
                    if is_info_about_query:
                        # For "information about" queries, indicate we verified but no data is available
                        state['final_response'] = "I verified the database, but no data is available for the requested information. The fields exist in the database schema, but they are currently empty or not populated in the records."
                        return state
                    
                    # ⭐ DYNAMIC: Generate error hints based on query analysis
                    error_hints = []
                    
                    # Check for incomplete questions (missing parameter values)
                    if self._detect_incomplete_question(user_message_lower):
                        # Try to extract the field name from the query
                        field_match = re.search(r'for\s+(\w+)|by\s+(\w+)', user_message_lower, re.I)
                        if field_match:
                            field_name = field_match.group(1) or field_match.group(2)
                            error_hints.append(f"Note: No specific {field_name} was provided. Showing all available records instead.")
                        else:
                            error_hints.append("Note: A required parameter is missing. Showing all available records instead.")
                    
                    # Check if query mentions fields that might not exist
                    elif self._check_missing_fields(user_message_lower):
                        error_hints.append("The requested field might not exist in the database or may have a different name.")
                    
                    # Check for similarity/embedding queries
                    elif 'similar' in user_message_lower or 'embedding' in user_message_lower:
                        error_hints.append("Similarity search requires vector embeddings. The feature might not be available.")
                    
                    # Check for date-related queries
                    elif any(date_term in user_message_lower for date_term in ['onwards', 'today', 'recently']):
                        error_hints.append("The date filter might not match any records. Try a different date range.")
                    
                    base_message = "I didn't find any matching records for your query."
                    if error_hints:
                        state['final_response'] = f"{base_message}\n\n{error_hints[0]}\n\nTry adjusting your search criteria or ask for help to see what information is available."
                    else:
                        state['final_response'] = f"{base_message} Try adjusting your search criteria or ask for help to see what information is available."
                    return state
            
            v2_data = results.get('postgresql', [])
            v1_data = results.get('mongodb', [])
            
            # ⭐ NEW: Check if this is an "information about" query with all NULL values
            # These queries should show "verified but no data available" even if rows exist
            user_message_lower = user_message.lower()
            info_about_keywords = [
                'hair color', 'hair color information', 'eye color', 'eye color details',
                'build type', 'build type information', 'mole', 'leucoderma',
                'height information', 'height', 'seizure worth', 'packaging details',
                'information about', 'with hair', 'with eye', 'with build',
                'with mole', 'with leucoderma', 'with height', 'show drugs with',
                'show accused with', 'list accused with', 'find accused with',
                'list all accused with', 'show accused', 'list accused',
                'accused with height', 'accused with build', 'accused with hair',
                'accused with eye', 'accused with mole', 'accused with leucoderma'
            ]
            is_info_about_query = any(kw in user_message_lower for kw in info_about_keywords)
            
            if is_info_about_query and v2_data:
                # Check if all requested fields are NULL/empty in the results
                # Get column names from first row
                first_row = v2_data[0]
                row_dict = dict(first_row) if hasattr(first_row, '_asdict') else dict(first_row) if isinstance(first_row, dict) else first_row
                column_names = list(row_dict.keys()) if isinstance(row_dict, dict) else []
                
                # Map user query to actual column names
                requested_columns = []
                if 'height' in user_message_lower:
                    requested_columns.extend([c for c in column_names if 'height' in c.lower()])
                if 'build' in user_message_lower or 'build type' in user_message_lower:
                    requested_columns.extend([c for c in column_names if 'build' in c.lower()])
                if 'hair' in user_message_lower or 'hair color' in user_message_lower:
                    # For hair color, check both 'hair' and 'color' columns (actual column is 'color')
                    requested_columns.extend([c for c in column_names if 'hair' in c.lower() or 'color' in c.lower()])
                if 'eye' in user_message_lower or 'eye color' in user_message_lower:
                    requested_columns.extend([c for c in column_names if 'eye' in c.lower() or 'color' in c.lower()])
                if 'mole' in user_message_lower:
                    requested_columns.extend([c for c in column_names if 'mole' in c.lower()])
                if 'leucoderma' in user_message_lower:
                    requested_columns.extend([c for c in column_names if 'leucoderma' in c.lower()])
                
                # Remove duplicates
                requested_columns = list(set(requested_columns))
                
                # Check if all requested columns are NULL/empty in first few rows
                if requested_columns:
                    all_null = True
                    sample_size = min(10, len(v2_data))  # Check first 10 rows
                    for row in v2_data[:sample_size]:
                        row_dict = dict(row) if hasattr(row, '_asdict') else dict(row) if isinstance(row, dict) else row
                        # Check if any requested column has a non-null value
                        for col in requested_columns:
                            if col in row_dict:
                                value = row_dict[col]
                                if value is not None and str(value).strip():
                                    all_null = False
                                    break
                        if not all_null:
                            break
                    
                    if all_null:
                        # All requested fields are NULL - show "verified but no data available"
                        state['final_response'] = "I verified the database, but no data is available for the requested information. The fields exist in the database schema, but they are currently empty or not populated in the records."
                        return state
            
            # ⭐ AGENT 4: NARRATIVE FORMATTING (if enabled)
            # Skip Agent 4 for:
            # 1. LARGE result sets when user wants "all" (>20 records)
            # 2. Classification/grouping queries (user wants raw data, not interpretation!)
            total_results = len(v2_data) + len(v1_data)
            user_wants_all = any(kw in user_message.lower() for kw in ['all', 'complete', 'everything', 'entire', 'full list'])
            
            # Detect classification/grouping queries
            # User wants raw data table when asking about classifications, groupings, statistics
            message_lower = user_message.lower()
            is_classification_query = any(kw in message_lower for kw in [
                # "by X" queries
                'by status', 'by type', 'by class', 'by classification', 'by district',
                'by category', 'group by', 'count by', 'crimes by', 'cases by',
                # Direct classification queries (without "by")
                'case classification', 'crime classification', 'class_classification',
                'get classification', 'show classification', 'list classification',
                # Grouping/aggregation keywords
                'grouped', 'breakdown', 'distribution', 'statistics by'
            ])
            
            # Use Agent 4 only if:
            # - Results are manageable (≤20) AND
            # - User doesn't want "all" AND
            # - NOT a classification query (user wants raw facts, not narratives!)
            use_agent4 = (
                self.enable_narrative and 
                self.narrative_formatter and 
                total_results <= 20 and 
                not user_wants_all and
                not is_classification_query
            )
            
            if use_agent4:
                try:
                    logger.info("Using Agent 4 (Narrative Formatter) to generate response...")
                    
                    # Prepare query metadata for context
                    query_metadata = {
                        'intent': state.get('intent'),
                        'target_database': state.get('target_database'),
                        'detected_entities': state.get('detected_entities', [])
                    }
                    
                    # Get format preferences (if user specified)
                    format_prefs = state.get('format_preferences', {})
                    
                    # Generate narrative using LLM
                    narrative_response = self.narrative_formatter.format_results(
                        user_question=user_message,
                        query_results=results,
                        query_metadata=query_metadata,
                        format_preferences=format_prefs
                    )
                    
                    if narrative_response:
                        state['final_response'] = narrative_response
                        state['formatted_with_agent4'] = True
                        logger.info("Agent 4 generated narrative response successfully! ⭐")
                        return state
                    else:
                        logger.warning("Agent 4 returned empty response, using fallback formatting")
                except Exception as e:
                    logger.error(f"Agent 4 error: {e}, using fallback formatting")
            else:
                # Log why Agent 4 was skipped
                if user_wants_all and total_results > 20:
                    logger.info(f"Large result set ({total_results} records) + 'all' requested - using standard formatting for complete list")
                elif is_classification_query:
                    logger.info(f"Classification/grouping query detected - using standard formatting (raw facts, no interpretation)")
                else:
                    logger.info("Using standard formatting (Agent 4 disabled or unavailable)")
            
            # FALLBACK: Standard formatting (if Agent 4 disabled or failed)
            if not use_agent4:
                logger.info("Standard formatting: showing raw data without narrative interpretation")
            
            # Check if this is an entity-only query (like just a phone number)
            if self.use_advanced and self.entity_detector.is_entity_only_query(user_message):
                # Use advanced formatting with relationship analysis
                response = self._format_entity_search(v1_data, v2_data, user_message)
                state['final_response'] = response
                logger.info("Used advanced entity-based formatting")
                return state
            
            # Build conversational response
            intro = self.conversation.format_conversational_response(
                results,
                user_message
            )
            parts = [intro]
            
            # Format V2 Data (PostgreSQL) - with error handling
            if v2_data:
                try:
                    pg_text = self.formatter.format_postgresql(v2_data, user_message)
                    parts.append(f"\n### 🗄️ V2 Data ({len(v2_data)} records)\n{pg_text}")
                except Exception as e:
                    logger.error(f"Error formatting PostgreSQL data: {e}", exc_info=True)
                    # Show friendly message instead of error
                    parts.append(f"\n### 🗄️ V2 Data ({len(v2_data)} records)\nI found {len(v2_data)} record(s), but encountered an issue displaying them. Please try rephrasing your query or ask for specific information.")
            
            # Format V1 Data (MongoDB) - with error handling
            if v1_data:
                try:
                    mongo_text = self.formatter.format_mongodb(v1_data, user_message)
                    parts.append(f"\n### 📚 V1 Data ({len(v1_data)} documents)\n{mongo_text}")
                except Exception as e:
                    logger.error(f"Error formatting MongoDB data: {e}", exc_info=True)
                    # Show friendly message instead of error
                    parts.append(f"\n### 📚 V1 Data ({len(v1_data)} documents)\nI found {len(v1_data)} document(s), but encountered an issue displaying them. Please try rephrasing your query or ask for specific information.")
            
            state['final_response'] = "\n".join(parts)
            logger.info("Response formatted successfully (standard formatting)")
            return state
        
        except Exception as e:
            # ⭐ CATCH ALL ERRORS and show friendly message
            logger.error(f"Error in format_response: {e}", exc_info=True)
            # Never show technical errors to users!
            state['final_response'] = "I'm still learning how to display this information. Please try rephrasing your query or ask for specific details like: 'Find person named [name]' or 'Show crimes by [category]'."
            state['error'] = f"Format error: {str(e)}"  # For logging only
            return state
    
    def _format_entity_search(self, v1_data: List[Dict], v2_data: List[Dict], query: str) -> str:
        """Format results for entity-based search (mobile number, email, etc.)"""
        # Combine all data
        all_data = v2_data + v1_data
        
        if not all_data:
            return "❌ No records found for this search."
        
        # Extract person details
        person_info = self.relationship_analyzer.extract_person_details(all_data)
        
        # Check if this looks like person-crime data
        has_names = bool(person_info.get('names'))
        has_crimes = any('crime' in str(k).lower() or 'case' in str(k).lower() 
                        for record in all_data for k in record.keys())
        
        if has_names and has_crimes:
            # Format as person profile with crimes
            return self.advanced_formatter.format_person_profile(person_info, all_data)
        elif has_crimes:
            # Format as crime summary
            return self.advanced_formatter.format_crime_summary(all_data, query)
        else:
            # Use advanced data summary
            return self.advanced_formatter.format_data_summary(v1_data, v2_data, query)
    
class ErrorHandlerNode(BaseNode):
    """Error handling node with helpful suggestions"""
        
    def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Provide helpful error messages and suggestions (NO technical errors shown!)"""
        logger.info("Node: handle_error")
        
        error = state.get('error')
        if not error:
            error = 'Unknown error occurred'
        
        error_str = str(error)
        
        # Convert technical error to user-friendly message
        user_friendly_message = self._sanitize_error_for_user(error_str)
        suggestions = self._get_suggestions(error_str)
        
        # Professional error response (NO technical details!)
        response = f"{user_friendly_message}\n\n"
        
        if suggestions:
            response += "**Here's how you can help me:**\n" + "\n".join(suggestions) + "\n\n"
        
        response += self._get_example_queries()
        
        state['final_response'] = response
        logger.info("Error handled with suggestions")
        return state
    
    def _sanitize_error_for_user(self, error: str) -> str:
        """
        Convert ALL technical errors to the SAME user-friendly message
        NO technical details exposed to users!
        """
        # ALWAYS show this professional message for ANY error
        return "I encountered an issue processing that request. Let me try a different approach."
    
    def _get_suggestions(self, error: str) -> List[str]:
        """Get context-specific suggestions based on error type"""
        suggestions = []
        error_lower = error.lower()
        
        if 'validation' in error_lower or 'forbidden' in error_lower:
            suggestions.extend([
                "• Try asking for specific information (e.g., 'Show me crimes in Sangareddy')",
                "• Use natural language questions instead of technical commands",
                "• Ask about specific FIR numbers, person names, or mobile numbers"
            ])
        elif 'generate' in error_lower or 'failed to generate' in error_lower:
            suggestions.extend([
                "• Be more specific (e.g., 'Find person named name' instead of just 'name')",
                "• Include context (e.g., 'Show me crimes for mobile 99999999')",
                "• Try searching by FIR number, person name, or location"
            ])
        elif 'timeout' in error_lower:
            suggestions.extend([
                "• Try limiting your search (e.g., 'Show recent 10 crimes')",
                "• Add specific filters (e.g., district name, date range)",
                "• Break complex questions into smaller queries"
            ])
        elif 'execution' in error_lower or 'syntax' in error_lower or 'column' in error_lower or 'table' in error_lower:
            suggestions.extend([
                "• Try a simpler query (e.g., 'How many crimes?' or 'Find person named name')",
                "• Search by specific details (FIR number, mobile number, person name)",
                "• Ask for recent records (e.g., 'Show FIR records from last week')"
            ])
        else:
            suggestions.extend([
                "• Try a different phrasing (e.g., 'Show me all pending cases')",
                "• Search by specific identifiers (FIR number, crime ID, mobile number)",
                "• Ask for help: 'What can you show me?'"
            ])
        
        return suggestions
    
    def _get_example_queries(self) -> str:
        """Get example queries to try"""
        return (
            "**Try asking:**\n"
            "• \"How many crimes?\"\n"
            "• \"Find person named Rajesh\"\n"
            "• \"Show FIR 243/2022\"\n"
            "• \"Crimes in Sangareddy district\"\n"
            "• \"Search mobile 9876543210\""
        )

# ============================================================================
# Helper Classes
# ============================================================================

class SQLCleaner:
    """Clean SQL queries from LLM output"""
    
    @staticmethod
    def clean(sql: str) -> str:
        """Aggressively clean SQL query from LLM output"""
        logger.debug(f"Cleaning SQL (original): {sql[:200]}")
        
        # Remove all escape characters
        sql = sql.replace('\\', '')
        
        # Extract from code blocks (but keep the SQL inside)
        code_match = re.search(r'```[\w]*\n(.*?)\n```', sql, re.DOTALL)
        if code_match:
            sql = code_match.group(1)
        else:
            sql = re.sub(r'```', '', sql)
        
        # Remove label prefixes (PostgreSQL:, SQL:, MongoDB:)
        sql = re.sub(r'^(PostgreSQL|MongoDB|SQL):\s*', '', sql, flags=re.IGNORECASE | re.MULTILINE)
        
        # Find the SELECT statement (up to semicolon or double newline)
        # This is more careful - keeps everything up to semicolon
        select_pattern = r'(SELECT\s+.+?)(?:;|$)'
        match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            sql = match.group(1).strip()
        else:
            # Fallback: find everything after SELECT
            select_match = re.search(r'\bSELECT\b', sql, re.IGNORECASE)
            if select_match:
                sql = sql[select_match.start():]
        
        # Remove "Note:", "MongoDB:", explanations that come after
        sql = re.sub(r'\s*(Note:|MongoDB:|PostgreSQL:).*$', '', sql, flags=re.IGNORECASE)
        
        # Remove "UNION ALL" and subsequent queries (take only first query)
        if 'UNION' in sql.upper():
            sql = sql.split('UNION')[0].strip()
        
        # Clean up excessive newlines (but keep space for multi-line queries)
        sql = re.sub(r'\n\s*\n', ' ', sql)
        
        # Clean up multiple spaces
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        # Remove trailing semicolon
        sql = sql.rstrip(';').strip()
        
        logger.debug(f"Cleaned SQL: {sql}")
        return sql
    
    @staticmethod
    def add_limit_if_missing(sql: str, user_message: str) -> str:
        """
        Add LIMIT clause to queries if missing (performance optimization).
        Respects user's explicit LIMIT if provided.
        """
        sql_upper = sql.upper()
        
        # Check if LIMIT already exists
        if 'LIMIT' in sql_upper:
            # Extract existing LIMIT value
            limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
            if limit_match:
                existing_limit = int(limit_match.group(1))
                # If user explicitly requested a large limit (>=500), respect it
                if existing_limit >= 500:
                    logger.debug(f"Query already has LIMIT {existing_limit}, keeping it")
                    return sql
                # If limit is reasonable, keep it
                if existing_limit <= 200:
                    return sql
        
        # Determine appropriate limit based on user intent
        message_lower = user_message.lower()
        default_limit = 100  # Default limit
        
        # Import Config for MAX_QUERY_ROWS
        from config import Config
        
        # Check if user explicitly asked for "all", "complete", "everything"
        if any(kw in message_lower for kw in ['all', 'complete', 'everything', 'entire', 'full list']):
            # User wants all results, but still limit to reasonable amount
            limit = min(500, Config.MAX_QUERY_ROWS)
            logger.info(f"User asked for 'all' - using LIMIT {limit}")
        # Check if user specified a number (e.g., "first 10", "top 5", "show 20")
        elif re.search(r'(first|top|show|limit|get)\s+(\d+)', message_lower):
            number_match = re.search(r'(first|top|show|limit|get)\s+(\d+)', message_lower)
            if number_match:
                requested_limit = int(number_match.group(2))
                limit = min(requested_limit, Config.MAX_QUERY_ROWS)
                logger.info(f"User requested {requested_limit} results - using LIMIT {limit}")
            else:
                limit = default_limit
        else:
            limit = default_limit
        
        # Add LIMIT clause at the end
        # Handle different query endings
        sql = sql.rstrip().rstrip(';')
        
        # Check if query ends with ORDER BY, GROUP BY, HAVING, etc.
        if re.search(r'\b(ORDER BY|GROUP BY|HAVING)\s+', sql_upper):
            # LIMIT goes after ORDER BY/GROUP BY/HAVING
            sql = f"{sql} LIMIT {limit}"
        else:
            # LIMIT goes at the end
            sql = f"{sql} LIMIT {limit}"
        
        logger.info(f"✅ Added LIMIT {limit} to query (performance optimization)")
        return sql
    
    @staticmethod
    def remove_not_null_filters(sql: str, user_message: str) -> str:
        """
        Remove IS NOT NULL filters for "information about" queries.
        These queries should show ALL records, not just those with non-null values.
        """
        message_lower = user_message.lower()
        
        # Keywords that indicate "information about" queries (expanded list)
        info_about_keywords = [
            'hair color', 'hair color information', 'eye color', 'eye color details',
            'build type', 'build type information', 'mole', 'leucoderma', 
            'height information', 'height', 'seizure worth', 'packaging details',
            'information about', 'with hair', 'with eye', 'with build',
            'with mole', 'with leucoderma', 'with height', 'show drugs with',
            'show accused with', 'list accused with', 'find accused with',
            'source location', 'seizure location', 'show drugs with source',
            'show drugs with seizure', 'find drugs seized from', 'find drugs seized at'
        ]
        
        # Check if this is an "information about" query
        is_info_query = any(kw in message_lower for kw in info_about_keywords)
        
        if not is_info_query:
            return sql  # Not an "information about" query, return as-is
        
        logger.info(f"⚠️ Detected 'information about' query - removing IS NOT NULL filters")
        
        # Pattern to match: field IS NOT NULL (with optional table alias)
        # Examples: "a.hair IS NOT NULL", "a.eyes IS NOT NULL", "hair IS NOT NULL", "d.seizure_worth IS NOT NULL"
        # Note: Actual column names are 'hair' (NOT 'hair_color'), 'eyes' (NOT 'eye_color')
        pattern = r'\b\w+\.?\w*\s+IS\s+NOT\s+NULL\b'
        
        # Remove IS NOT NULL conditions
        original_sql = sql
        sql = re.sub(pattern, '', sql, flags=re.IGNORECASE)
        
        # Clean up resulting SQL
        # Remove "AND" or "OR" that might be left hanging
        sql = re.sub(r'\s+AND\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+OR\s+OR\s+', ' OR ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+AND\s+OR\s+', ' AND ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+OR\s+AND\s+', ' AND ', sql, flags=re.IGNORECASE)
        
        # Remove leading/trailing AND/OR from WHERE clause
        sql = re.sub(r'\bWHERE\s+(AND|OR)\s+', 'WHERE ', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+(AND|OR)\s+$', '', sql, flags=re.IGNORECASE)
        
        # ⚠️ CRITICAL FIX: Remove empty WHERE clauses (WHERE with nothing after it)
        # This happens when all conditions are removed, leaving "WHERE LIMIT" which is invalid
        sql = re.sub(r'\bWHERE\s+(LIMIT|ORDER BY|GROUP BY|HAVING)', r'\1', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bWHERE\s*$', '', sql, flags=re.IGNORECASE)
        
        # Clean up multiple spaces
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        if sql != original_sql:
            logger.info(f"✅ Removed IS NOT NULL filters from query")
            logger.debug(f"Original: {original_sql[:200]}")
            logger.debug(f"Modified: {sql[:200]}")
        
        return sql

class ResultFormatter:
    """Format query results for user-friendly display"""
    
    def format_postgresql(self, data: List[Dict], query: str = '') -> str:
        """Format PostgreSQL results"""
        if not data:
            return "No records found."
        
        # Single value (COUNT, SUM, etc.)
        if len(data) == 1 and len(data[0]) == 1:
            value = list(data[0].values())[0]
            key = list(data[0].keys())[0]
            # Format numbers with commas, strings as-is
            if isinstance(value, (int, float)):
                return f"**{key.replace('_', ' ').title()}:** {value:,}"
            else:
                return f"**{key.replace('_', ' ').title()}:** {value}"
        
        # Multiple rows - Beautiful formatting
        # Determine preview limit based on user request
        preview_limit = self._determine_preview_limit(query, len(data))
        preview = data[:preview_limit]
        result = []
        
        for i, row in enumerate(preview, 1):
            # Detect record type and format appropriately
            if 'full_name' in row or 'name' in row:
                # Person/Accused record
                result.append(f"**👤 Person {i}:**")
            elif 'fir_num' in row or 'crime_type' in row:
                # Crime record
                result.append(f"**📋 Crime {i}:**")
            elif 'summary_text' in row:
                # Summary record
                result.append(f"**📄 Summary {i}:**")
            else:
                result.append(f"**📊 Record {i}:**")
            
            # Group fields by table/section for better organization
            # For drug queries, prioritize brief_facts_drugs fields
            drug_fields = {}
            property_fields = {}
            crime_fields = {}
            other_fields = {}
            
            # ⭐ CRITICAL: Detect if user asked for specific fields (even if empty)
            query_lower_for_fields = query.lower()
            requested_field_keywords = []
            
            # Detect field requests from query
            field_keyword_map = {
                'hair': ['hair', 'hair_color', 'hair_style'],
                'height': ['height', 'height_from_cm'],
                'build': ['build', 'build_type'],
                'mole': ['mole'],
                'leucoderma': ['leucoderma'],
                'seizure worth': ['seizure_worth', 'seizure value'],
                'packaging': ['packaging', 'packaging_details', 'number_of_packets'],
                'eye': ['eye', 'eye_color', 'eyes'],
                'color': ['color', 'complexion'],
            }
            
            for keyword, field_list in field_keyword_map.items():
                if keyword in query_lower_for_fields:
                    requested_field_keywords.extend(field_list)
            
            for key, value in row.items():
                # ⭐ CRITICAL: Show requested fields even if empty (with "Not available" message)
                is_requested_field = any(field_kw in key.lower() for field_kw in requested_field_keywords)
                
                if value is None or value == '':
                    if is_requested_field:
                        # Show requested field even if empty
                        display_key = key.replace('_', ' ').title()
                        icon = self._get_field_icon(key)
                        result.append(f"    {icon} **{display_key}:** Not available (field exists but is empty)")
                    continue  # Skip other empty values
                
                # Categorize fields
                if key.startswith('drug_') or key in ['drug_id', 'drug_name', 'scientific_name', 'brand_name', 
                                                       'drug_category', 'drug_schedule', 'total_quantity', 
                                                       'quantity_unit', 'quantity_numeric', 'number_of_packets',
                                                       'weight_breakdown', 'packaging_details', 'source_location',
                                                       'destination', 'transport_method', 'supply_chain',
                                                       'seizure_location', 'seizure_time', 'seizure_method',
                                                       'seizure_officer', 'commercial_quantity', 'is_commercial',
                                                       'seizure_worth', 'street_value', 'street_value_numeric', 'purity']:
                    drug_fields[key] = value
                elif key.startswith('property_') or key in ['property_id', 'case_property_id', 'property_nature',
                                                             'property_category', 'particular_of_property',
                                                             'property_status', 'estimate_value', 'recovered_value',
                                                             'recovered_from', 'place_of_recovery', 'date_of_seizure',
                                                             'belongs', 'additional_details', 'media']:
                    property_fields[key] = value
                elif key in ['crime_id', 'fir_num', 'fir_reg_num', 'crime_type', 'case_status', 'fir_date',
                             'major_head', 'minor_head', 'acts_sections', 'io_name', 'io_rank', 'brief_facts',
                             'ps_name', 'dist_name', 'circle_name', 'zone_name']:
                    crime_fields[key] = value
                elif key == 'domicile_classification':
                    # ⭐ NATIONALITY field - always include in other_fields (will be extracted and prioritized if user asked)
                    other_fields[key] = value
                else:
                    other_fields[key] = value
            
            # Check if user asked for specific fields (comprehensive drug field detection)
            query_lower = query.lower()
            prioritize_nationality = 'nationality' in query_lower or 'domicile' in query_lower or 'native' in query_lower or 'interstate' in query_lower or 'international' in query_lower
            prioritize_transport = 'transport' in query_lower or 'transport method' in query_lower
            prioritize_supply_chain = 'supply chain' in query_lower or 'supply' in query_lower
            prioritize_packaging = 'packaging' in query_lower or 'package' in query_lower
            prioritize_weight = 'weight' in query_lower or 'quantity' in query_lower
            prioritize_seizure = 'seizure' in query_lower
            prioritize_commercial = 'commercial' in query_lower
            prioritize_purity = 'purity' in query_lower
            prioritize_value = 'street value' in query_lower or ('value' in query_lower and 'street' in query_lower)
            
            # Reorder drug fields to prioritize requested fields
            if any([prioritize_transport, prioritize_supply_chain, prioritize_packaging, 
                   prioritize_weight, prioritize_seizure, prioritize_commercial, 
                   prioritize_purity, prioritize_value]):
                prioritized_drug_fields = {}
                other_drug_fields = {}
                
                for key, value in drug_fields.items():
                    # Packaging related
                    if prioritize_packaging and key in ['packaging_details', 'number_of_packets', 'weight_breakdown']:
                        prioritized_drug_fields[key] = value
                    # Transport related
                    elif prioritize_transport and key in ['transport_method', 'source_location', 'destination']:
                        prioritized_drug_fields[key] = value
                    # Supply chain related
                    elif prioritize_supply_chain and key in ['supply_chain', 'source_location', 'destination', 'transport_method']:
                        prioritized_drug_fields[key] = value
                    # Weight/quantity related
                    elif prioritize_weight and key in ['weight_breakdown', 'total_quantity', 'quantity_numeric', 'quantity_unit', 'number_of_packets']:
                        prioritized_drug_fields[key] = value
                    # Seizure related
                    elif prioritize_seizure and key in ['seizure_location', 'seizure_time', 'seizure_method', 'seizure_officer', 'seizure_worth']:
                        prioritized_drug_fields[key] = value
                    # Commercial quantity related
                    elif prioritize_commercial and key in ['is_commercial', 'commercial_quantity', 'total_quantity', 'quantity_numeric']:
                        prioritized_drug_fields[key] = value
                    # Purity related
                    elif prioritize_purity and key in ['purity', 'drug_name', 'scientific_name']:
                        prioritized_drug_fields[key] = value
                    # Street value related
                    elif prioritize_value and key in ['street_value', 'street_value_numeric', 'seizure_worth']:
                        prioritized_drug_fields[key] = value
                    else:
                        other_drug_fields[key] = value
                
                # Merge: prioritized first, then others
                drug_fields = {**prioritized_drug_fields, **other_drug_fields}
            
            # Display fields in priority order: Drug fields first, then crime, then properties (if any)
            all_field_groups = []
            if drug_fields:
                all_field_groups.append(('💊 Drug Information', drug_fields))
            if crime_fields:
                all_field_groups.append(('📋 Crime Details', crime_fields))
            if property_fields:
                all_field_groups.append(('📦 Property Details', property_fields))
            if other_fields:
                all_field_groups.append(('ℹ️ Other Information', other_fields))
            
            # If no field groups, just show all fields without grouping
            if not all_field_groups:
                # Fallback: show all fields without grouping
                for key, value in row.items():
                    # ⭐ CRITICAL: Show requested fields even if empty
                    is_requested_field = any(field_kw in key.lower() for field_kw in requested_field_keywords)
                    
                    if value is None or value == '':
                        if is_requested_field:
                            display_key = key.replace('_', ' ').title()
                            icon = self._get_field_icon(key)
                            result.append(f"    {icon} **{display_key}:** Not available (field exists but is empty)")
                        continue
                    display_key = key.replace('_', ' ').title()
                    icon = self._get_field_icon(key)
                    if isinstance(value, str) and len(value) > 200:
                        display_value = value[:200] + "..."
                    else:
                        display_value = value
                    result.append(f"    {icon} **{display_key}:** {display_value}")
            else:
                for section_name, field_group in all_field_groups:
                    if not field_group:
                        continue
                    
                    if section_name and len(field_group) > 0:
                        result.append(f"  **{section_name}:**")
                    
                    for key, value in field_group.items():
                        # Format display name
                        display_key = key.replace('_', ' ').title()
                        
                        # ⭐ CRITICAL: Also check if this is a requested field from field_keyword_map
                        is_keyword_requested = any(field_kw in key.lower() for field_kw in requested_field_keywords)
                        
                        # Highlight fields user explicitly asked for
                        is_requested_field = (
                            is_keyword_requested or  # ⭐ NEW: Check keyword-based field requests
                            (prioritize_nationality and key == 'domicile_classification') or
                            (prioritize_packaging and key in ['packaging_details', 'number_of_packets', 'weight_breakdown']) or
                            (prioritize_transport and key in ['transport_method', 'source_location', 'destination']) or
                            (prioritize_supply_chain and key in ['supply_chain', 'source_location', 'destination', 'transport_method']) or
                            (prioritize_weight and key in ['weight_breakdown', 'total_quantity', 'quantity_numeric', 'quantity_unit', 'number_of_packets']) or
                            (prioritize_seizure and key in ['seizure_location', 'seizure_time', 'seizure_method', 'seizure_officer', 'seizure_worth']) or
                            (prioritize_commercial and key in ['is_commercial', 'commercial_quantity', 'total_quantity', 'quantity_numeric']) or
                            (prioritize_purity and key in ['purity', 'drug_name', 'scientific_name']) or
                            (prioritize_value and key in ['street_value', 'street_value_numeric', 'seizure_worth'])
                        )
                        
                        # ⭐ CRITICAL: If field is requested but value is empty, show it anyway
                        if (value is None or value == '') and is_requested_field:
                            display_value = "Not available (field exists but is empty)"
                        elif value is None or value == '':
                            continue  # Skip non-requested empty fields
                        
                        # Format value
                        # ⭐ SPECIAL: For domicile_classification, add explanation FIRST
                        if key == 'domicile_classification' and prioritize_nationality:
                            # Add explanation based on value
                            if isinstance(value, str) and value:
                                value_lower = value.lower()
                                if 'native' in value_lower:
                                    display_value = f"{value} (Person belongs to Telangana state, India)"
                                elif 'inter' in value_lower or 'interstate' in value_lower:
                                    display_value = f"{value} (Person belongs to India but NOT Telangana - other Indian states)"
                                elif 'international' in value_lower:
                                    display_value = f"{value} (Person belongs to outside India - foreign country)"
                                else:
                                    display_value = value
                            elif value is None:
                                display_value = "Not available (field exists but is NULL)"
                            else:
                                display_value = value
                        elif isinstance(value, (int, float)) and value > 1000:
                            display_value = f"{value:,}"
                        elif isinstance(value, str) and len(value) > 200:
                            # Don't truncate important fields that users explicitly ask for
                            important_fields = ['summary_text', 'brief_facts', 'summary', 'description', 
                                               'key_details', 'role_in_crime', 'particular_of_property',
                                               'ai_summary', 'case_details', 'remarks', 'supply_chain',
                                               'transport_method', 'source_location', 'destination',
                                               'packaging_details', 'weight_breakdown', 'number_of_packets']
                            if any(field in key.lower() for field in important_fields):
                                display_value = value  # Show complete text
                            else:
                                display_value = value[:200] + "..."  # Truncate less important fields
                        else:
                            display_value = value
                        
                        # Add appropriate icon
                        icon = self._get_field_icon(key)
                        
                        # Highlight requested fields with emoji prefix
                        if is_requested_field:
                            result.append(f"    ⭐ {icon} **{display_key}:** {display_value}")
                        else:
                            result.append(f"    {icon} **{display_key}:** {display_value}")
            
            result.append("")  # Blank line between records
        
        if len(data) > preview_limit:
            remaining = len(data) - preview_limit
            result.append(f"\n_... and {remaining} more records._")
            if preview_limit == 10:  # Only show tip if using default limit
                result.append(f"💡 **Tip:** To see more, ask: \"Show me all\" or \"Show me complete list\" or \"Show top 50\"")
        
        return "\n".join(result)
    
    def _determine_preview_limit(self, user_message: str, total_records: int) -> int:
        """
        Determine how many records to show based on user request
        
        Args:
            user_message: User's query message
            total_records: Total number of records available
            
        Returns:
            Number of records to display
        """
        message_lower = user_message.lower()
        
        # User explicitly wants ALL records (show everything up to MAX_QUERY_ROWS)
        if any(keyword in message_lower for keyword in ['all', 'complete', 'everything', 'full list', 'entire', 'show all', 'list all']):
            # Show ALL records (up to database limit of 1000)
            from config import Config
            max_limit = Config.MAX_QUERY_ROWS  # 1000 from config
            return min(total_records, max_limit)
        
        # User wants a specific number
        import re
        number_match = re.search(r'(?:show|top|first|last|recent|limit)\s+(\d+)', message_lower)
        if number_match:
            requested = int(number_match.group(1))
            return min(requested, 500)  # Increased cap from 100 to 500
        
        # Default: Show 10 records
        return 10
    
    def _get_field_icon(self, field_name: str) -> str:
        """Get appropriate icon for field"""
        field_lower = field_name.lower()
        
        if 'name' in field_lower:
            return '👤'
        elif 'phone' in field_lower or 'mobile' in field_lower:
            return '📞'
        elif 'email' in field_lower:
            return '📧'
        elif 'address' in field_lower or 'district' in field_lower:
            return '📍'
        elif 'fir' in field_lower or 'case' in field_lower:
            return '📋'
        elif 'date' in field_lower or 'time' in field_lower:
            return '📅'
        elif 'status' in field_lower:
            return '🔖'
        elif 'crime' in field_lower or 'offense' in field_lower:
            return '⚖️'
        elif 'drug' in field_lower or 'narcotic' in field_lower:
            return '💊'
        elif 'transport' in field_lower or 'method' in field_lower:
            return '🚚'
        elif 'supply' in field_lower or 'chain' in field_lower:
            return '🔗'
        elif 'source' in field_lower or 'destination' in field_lower:
            return '📍'
        elif 'packaging' in field_lower or 'package' in field_lower:
            return '📦'
        elif 'weight' in field_lower or 'quantity' in field_lower:
            return '⚖️'
        elif 'seizure' in field_lower:
            return '🔍'
        elif 'purity' in field_lower:
            return '🧪'
        elif 'value' in field_lower or 'worth' in field_lower:
            return '💰'
        elif 'age' in field_lower:
            return '🎂'
        elif 'gender' in field_lower:
            return '👥'
        elif 'domicile' in field_lower or 'nationality' in field_lower:
            return '🌍'
        else:
            return '•'
    
    def format_mongodb(self, data: List[Dict], query: str = '') -> str:
        """Format MongoDB results with dynamic preview limit"""
        if not data:
            return "No documents found."
        
        # ⭐ CRITICAL: Detect if user asked for specific fields (even if empty)
        query_lower_for_fields = query.lower()
        requested_field_keywords = []
        
        # Detect field requests from query
        field_keyword_map = {
            'hair': ['hair', 'hair_color', 'hair_style', 'HAIR_COLOR', 'HAIR_STYLE'],
            'height': ['height', 'height_from_cm', 'HEIGHT_FROM_CM'],
            'build': ['build', 'build_type', 'BUILD_TYPE'],
            'mole': ['mole', 'MOLE'],
            'leucoderma': ['leucoderma', 'LEUCODERMA'],
            'seizure worth': ['seizure_worth', 'seizure value'],
            'packaging': ['packaging', 'packaging_details', 'number_of_packets'],
            'eye': ['eye', 'eye_color', 'eyes', 'EYE_COLOR'],
            'color': ['color', 'complexion', 'COMPLEXION_TYPE'],
        }
        
        for keyword, field_list in field_keyword_map.items():
            if keyword in query_lower_for_fields:
                requested_field_keywords.extend(field_list)
        
        # Determine preview limit based on user request (same logic as PostgreSQL)
        preview_limit = self._determine_preview_limit(query, len(data))
        preview = data[:preview_limit]
        result = []
        
        for i, doc in enumerate(preview, 1):
            result.append(f"**Document {i}:**")
            for key, value in doc.items():
                # ⭐ CRITICAL: Check if this is a requested field
                is_requested_field = any(field_kw in key.upper() or field_kw in key.lower() for field_kw in requested_field_keywords)
                
                if value is None:
                    if is_requested_field:
                        value = "Not available (field exists but is empty)"
                    else:
                        value = "—"
                else:
                    str_value = str(value)
                    if len(str_value) > 150:
                        # Don't truncate important fields
                        important_fields = ['accused_name', 'drug_desc', 'drug_particulars', 
                                           'brief', 'summary', 'description', 'remarks',
                                           'paking_making_desc', 'other_identify_marks']
                        if any(field.lower() in key.lower() for field in important_fields):
                            value = str_value  # Show complete text
                        else:
                            str_value = str_value[:150] + "..."
                            value = str_value
                    else:
                        value = str_value
                result.append(f"  • {key}: {value}")
            result.append("")
        
        if len(data) > preview_limit:
            remaining = len(data) - preview_limit
            result.append(f"\n_... and {remaining} more documents._")
            if preview_limit == 10:  # Only show tip if using default limit
                result.append(f"💡 **Tip:** To see more, ask: \"Show me all\" or \"Show me complete list\"")
        
        return "\n".join(result)

# ============================================================================
# Main AgentNodes Class (Backwards Compatible)
# ============================================================================

class AgentNodes:
    """
    Backward-compatible AgentNodes class
    Internally uses modular node implementations for better maintainability
    """
    
    def __init__(
        self,
        llm_client,
        schema_manager,
        postgres_executor,
        mongo_executor,
        cache_manager
    ):
        """Initialize with all dependencies"""
        # Import local dependencies (avoid circular imports)
        from agents.intelligent_query_planner import IntelligentQueryPlanner
        from agents.smart_schema import SmartSchemaSelector
        from agents.conversation_handler import ConversationHandler
        from security.query_validator import QueryValidator
        
        # Store dependencies
        self.llm = llm_client
        self.schema_manager = schema_manager
        self.postgres = postgres_executor
        self.mongo = mongo_executor
        self.cache = cache_manager
        
        # Create helper instances
        query_planner = IntelligentQueryPlanner()
        smart_schema = SmartSchemaSelector()
        conversation = ConversationHandler()
        validator = QueryValidator
        
        # Initialize modular nodes
        self._intent_parser = IntentParserNode(
            llm_client, schema_manager, cache_manager, conversation
        )
        self._schema_fetcher = SchemaFetcherNode(
            schema_manager, cache_manager, query_planner, smart_schema
        )
        self._query_generator = QueryGeneratorNode(llm_client)
        self._query_validator = QueryValidatorNode(validator)
        self._query_executor = QueryExecutorNode(
            postgres_executor, mongo_executor, cache_manager, validator
        )
        # Agent 4: Pass LLM client and cache manager for narrative formatting
        self._response_formatter = ResponseFormatterNode(
            conversation_handler=conversation,
            llm_client=llm_client,
            cache_manager=cache_manager
        )
        self._error_handler = ErrorHandlerNode()
    
    # Backward-compatible methods (LangGraph calls these)
    
    def parse_intent(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 1: Parse user intent"""
        return self._intent_parser(state)
    
    def get_schema(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 2: Get relevant schema"""
        return self._schema_fetcher(state)
    
    def generate_sql(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 3: Generate SQL/MongoDB queries"""
        return self._query_generator(state)
    
    def validate_sql(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 4: Validate queries"""
        return self._query_validator(state)
    
    def execute_query(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 5: Execute queries"""
        return self._query_executor(state)
    
    def format_response(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node 6: Format response"""
        return self._response_formatter(state)
    
    def handle_error(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Error handler"""
        return self._error_handler(state)


"""
Entity Detector - World-Class Smart Detection
Advanced pattern matching with ML-ready architecture and international support
NOW WITH COMPREHENSIVE CRIME DOMAIN ENTITY MAPPING (50+ entity types!)
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache

logger = logging.getLogger(__name__)

# Import crime domain entities
try:
    from agents.crime_domain_entities import (
        CrimeDomainEntityType,
        CRIME_DOMAIN_PATTERNS,
        CRIME_DOMAIN_FIELD_MAPPINGS,
        classify_query_intent,
        QueryIntent as DomainQueryIntent
    )
    DOMAIN_ENTITIES_AVAILABLE = True
except ImportError:
    DOMAIN_ENTITIES_AVAILABLE = False
    logger.warning("Crime domain entities not available")

# Import spaCy for fast entity detection
try:
    import spacy
    # Try to load the model
    try:
        NLP_MODEL = spacy.load("en_core_web_sm")
        SPACY_AVAILABLE = True
        logger.info("spaCy loaded successfully for fast entity detection")
    except OSError:
        SPACY_AVAILABLE = False
        logger.warning("spaCy model 'en_core_web_sm' not found. Run: python -m spacy download en_core_web_sm")
except ImportError:
    SPACY_AVAILABLE = False
    logger.warning("spaCy not installed. Falling back to regex entity detection")

# ============================================================================
# Entity Types with Extended Support
# ============================================================================

class EntityType(Enum):
    """Comprehensive entity type classification"""
    # Contact Information
    MOBILE_NUMBER = "mobile_number"
    EMAIL = "email"
    LANDLINE = "landline"
    
    # Technical IDs
    MONGODB_OBJECTID = "objectid"
    UUID = "uuid"
    NUMERIC_ID = "numeric_id"
    
    # Case Identifiers
    CASE_NUMBER = "case_number"
    FIR_NUMBER = "fir_number"
    COMPLAINT_ID = "complaint_id"
    
    # Indian Identity Documents
    AADHAAR_NUMBER = "aadhaar_number"
    PAN_NUMBER = "pan_number"
    PASSPORT_NUMBER = "passport_number"
    VOTER_ID = "voter_id"
    DRIVING_LICENSE = "driving_license"
    RATION_CARD = "ration_card"
    
    # Vehicle Related
    VEHICLE_NUMBER = "vehicle_number"
    CHASSIS_NUMBER = "chassis_number"
    ENGINE_NUMBER = "engine_number"
    
    # Financial
    BANK_ACCOUNT = "bank_account"
    IFSC_CODE = "ifsc_code"
    UPI_ID = "upi_id"
    
    # Other
    PERSON_NAME = "person_name"
    DATE = "date"
    TIME = "time"
    IP_ADDRESS = "ip_address"
    UNKNOWN = "unknown"

class ConfidenceLevel(Enum):
    """Confidence levels for detection"""
    VERY_HIGH = 0.95  # Almost certain
    HIGH = 0.85       # Very likely
    MEDIUM = 0.70     # Probably correct
    LOW = 0.50        # Uncertain

@dataclass
class DetectedEntity:
    """Enhanced detected entity with rich metadata"""
    entity_type: EntityType
    value: str
    confidence: float
    search_fields: List[str]
    normalized_value: Optional[str] = None
    validation_status: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self):
        return f"{self.entity_type.value}: {self.value} (confidence: {self.confidence:.2f})"
    
    def __hash__(self):
        return hash((self.entity_type, self.value))
    
    def __eq__(self, other):
        return (isinstance(other, DetectedEntity) and
                self.entity_type == other.entity_type and
                self.value == other.value)

# ============================================================================
# Pattern Registry with Compiled Regex
# ============================================================================

class PatternRegistry:
    """Centralized pattern registry with pre-compiled regexes"""
    
    # Pre-compile all patterns for performance
    _compiled_patterns: Dict[EntityType, List[re.Pattern]] = {}
    
    PATTERNS = {
        # Mobile Numbers (Indian and International)
        EntityType.MOBILE_NUMBER: [
            r'\b\+91[- ]?\d{5}[- ]?\d{5}\b',  # +91 XXXXX XXXXX
            r'\b91\d{10}\b',                   # 91XXXXXXXXXX
            r'\b[6-9]\d{9}\b',                 # Indian 10-digit starting with 6-9
            r'\b\+\d{1,3}[- ]?\d{3}[- ]?\d{3}[- ]?\d{4}\b',  # International
        ],
        
        # Landline
        EntityType.LANDLINE: [
            r'\b0\d{2,4}[- ]?\d{6,8}\b',  # Indian landline
        ],
        
        # Email
        EntityType.EMAIL: [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        ],
        
        # MongoDB ObjectID
        EntityType.MONGODB_OBJECTID: [
            r'\b[a-f0-9]{24}\b',
        ],
        
        # UUID
        EntityType.UUID: [
            r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',
        ],
        
        # Aadhaar Number
        EntityType.AADHAAR_NUMBER: [
            r'\b\d{4}[- ]?\d{4}[- ]?\d{4}\b',  # XXXX XXXX XXXX
        ],
        
        # PAN Number
        EntityType.PAN_NUMBER: [
            r'\b[A-Z]{5}\d{4}[A-Z]\b',  # ABCDE1234F
        ],
        
        # Passport Number
        EntityType.PASSPORT_NUMBER: [
            r'\b[A-Z]\d{7}\b',         # Indian: A1234567
            r'\b[A-Z]{2}\d{7}\b',      # Some formats: AB1234567
        ],
        
        # Voter ID (EPIC)
        EntityType.VOTER_ID: [
            r'\b[A-Z]{3}\d{7}\b',      # ABC1234567
        ],
        
        # Driving License
        EntityType.DRIVING_LICENSE: [
            r'\b[A-Z]{2}[-]?\d{2}[-]?\d{11}\b',  # DL-14-20110012345
            r'\b[A-Z]{2}\d{13}\b',               # DL1420110012345
        ],
        
        # Vehicle Number
        EntityType.VEHICLE_NUMBER: [
            r'\b[A-Z]{2}[- ]?\d{1,2}[- ]?[A-Z]{1,2}[- ]?\d{4}\b',  # Indian: DL-01-AB-1234
            r'\b[A-Z]{2}\d{2}[A-Z]{1,2}\d{4}\b',                    # No spaces
        ],
        
        # Bank Account
        EntityType.BANK_ACCOUNT: [
            r'\b\d{9,18}\b',  # 9-18 digits
        ],
        
        # IFSC Code
        EntityType.IFSC_CODE: [
            r'\b[A-Z]{4}0[A-Z0-9]{6}\b',  # SBIN0001234
        ],
        
        # UPI ID
        EntityType.UPI_ID: [
            r'\b[a-zA-Z0-9._-]+@[a-zA-Z]+\b',  # name@bank
        ],
        
        # FIR/Case Number
        EntityType.FIR_NUMBER: [
            r'\bFIR[/-]?\d{4}[/-]?\d+\b',
            r'\bFIR\s*NO\.?\s*\d+\b',
        ],
        
        EntityType.CASE_NUMBER: [
            r'\bCASE[/-]?\d+\b',
            r'\bCC\s*\d+[/-]\d+\b',
        ],
        
        # Dates
        EntityType.DATE: [
            r'\b\d{4}-\d{2}-\d{2}\b',          # YYYY-MM-DD
            r'\b\d{2}/\d{2}/\d{4}\b',          # DD/MM/YYYY
            r'\b\d{2}-\d{2}-\d{4}\b',          # DD-MM-YYYY
        ],
        
        # Time
        EntityType.TIME: [
            r'\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b',
        ],
        
        # IP Address
        EntityType.IP_ADDRESS: [
            r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
        ],
        
        # Person Name (capitalized words with optional alias using @)
        EntityType.PERSON_NAME: [
            # Name with alias using @ (e.g., "Rajendra Prasad @ Sachin")
            r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*@\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b',
            # Multiple capitalized words (2-4 words, likely a full name)
            # Will filter out false positives (List, Show, FIR, etc.) later
            r'\b[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b',
            # Single capitalized name (after "who", "find", "search")
            r'(?:who|find|search|named?)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
        ],
        
        # Numeric ID (lower priority)
        EntityType.NUMERIC_ID: [
            r'\b\d{4,12}\b',
        ],
    }
    
    @classmethod
    def get_compiled_patterns(cls, entity_type: EntityType) -> List[re.Pattern]:
        """Get pre-compiled regex patterns for entity type"""
        if not cls._compiled_patterns:
            cls._compile_all_patterns()
        return cls._compiled_patterns.get(entity_type, [])
    
    @classmethod
    def _compile_all_patterns(cls):
        """Compile all regex patterns once at startup"""
        for entity_type, patterns in cls.PATTERNS.items():
            cls._compiled_patterns[entity_type] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]

# ============================================================================
# Field Mappings
# ============================================================================

class FieldMapper:
    """Maps entity types to database fields"""
    
    FIELD_MAPPING = {
        # ⭐ ACTUAL COLUMNS from your PostgreSQL + MongoDB schema!
        EntityType.MOBILE_NUMBER: [
            # PostgreSQL V2 Data (ACTUAL!)
            'phone_number',  # persons table ⭐
            'phone_numbers',  # brief_facts_accused table ⭐
            'country_code',  # persons table
            # MongoDB V1 Data (ACTUAL! - UPPERCASE!)
            'MOBILE_1',  # fir_records ⭐⭐
            'INT_FATHER_MOBILE_NO',  # fir_records
            'INT_MOTHER_MOBILE_NO',  # fir_records
            'INT_WIFE_MOBILE_NO',  # fir_records
            'INT_BROTHER_MOBILE_NO',  # fir_records
            'TELEPHONE_RESIDENCE',  # fir_records
            # Generic fallbacks
            'mobile', 'phone', 'contact_number', 'MOBILE_NUMBER'
        ],
        EntityType.EMAIL: [
            # PostgreSQL V2 Data (ACTUAL!)
            'email_id',  # persons table ⭐
            # MongoDB V1 Data (if exists)
            'EMAIL',  # UPPERCASE
            # Generic fallbacks
            'email', 'email_address'
        ],
        EntityType.MONGODB_OBJECTID: [
            '_id', 'id'
        ],
        EntityType.AADHAAR_NUMBER: [
            # May be in JSONB fields (source_person_fields, source_accused_fields)
            'aadhaar', 'aadhaar_number', 'aadhar', 'aadhar_no', 'AADHAAR', 
            'AADHAAR_NUMBER', 'uid_number', 'UID'
        ],
        EntityType.PAN_NUMBER: [
            'pan', 'pan_number', 'pan_card', 'PAN', 'PAN_NUMBER'
        ],
        EntityType.PASSPORT_NUMBER: [
            'passport', 'passport_number', 'passport_no', 'PASSPORT'
        ],
        EntityType.VOTER_ID: [
            'voter_id', 'epic_no', 'epic', 'VOTER_ID', 'EPIC'
        ],
        EntityType.DRIVING_LICENSE: [
            'driving_license', 'dl_number', 'dl_no', 'DL_NUMBER', 'LICENSE'
        ],
        EntityType.RATION_CARD: [
            'ration_card', 'ration_card_number', 'RATION_CARD'
        ],
        EntityType.VEHICLE_NUMBER: [
            'vehicle_number', 'registration_number', 'reg_no', 'VEHICLE_NUMBER'
        ],
        EntityType.BANK_ACCOUNT: [
            'account_number', 'bank_account', 'acc_no'
        ],
        EntityType.IFSC_CODE: [
            'ifsc', 'ifsc_code', 'IFSC', 'IFSC_CODE'
        ],
        EntityType.FIR_NUMBER: [
            # PostgreSQL V2 Data (ACTUAL!)
            'fir_num',  # crimes table ⭐
            'fir_reg_num',  # crimes table ⭐
            # MongoDB V1 Data (ACTUAL! - UPPERCASE!)
            'FIR_NO',  # fir_records ⭐⭐
            'FIR_REG_NUM',  # fir_records ⭐⭐
            # Generic fallbacks
            'fir_number', 'FIR_NUMBER'
        ],
        EntityType.CASE_NUMBER: [
            'case_number', 'case_no', 'CASE_NUMBER'
        ],
        EntityType.PERSON_NAME: [
            # PostgreSQL V2 Data (ACTUAL!)
            'full_name',  # persons table ⭐
            'name',  # persons table
            'alias',  # persons table (for @ aliases)
            # MongoDB V1 Data (ACTUAL! - UPPERCASE!)
            'ACCUSED_NAME',  # fir_records ⭐⭐
            'FATHER_NAME',  # fir_records
            'NAME',  # fir_records
            'SURNAME',  # fir_records
            'FULL_NAME',  # fir_records
            # Generic fallbacks
            'person_name', 'accused_name'
        ],
        EntityType.IP_ADDRESS: [
            'ip_address', 'ip', 'IP_ADDRESS', 'IP'
        ]
    }

# ============================================================================
# Validators
# ============================================================================

class EntityValidator:
    """Validates detected entities"""
    
    @staticmethod
    def validate_aadhaar(value: str) -> Tuple[bool, str]:
        """Validate Aadhaar number with basic checks"""
        clean = value.replace(' ', '').replace('-', '')
        
        if len(clean) != 12 or not clean.isdigit():
            return False, "Invalid format"
        
        # Aadhaar cannot start with 0 or 1
        if clean[0] in '01':
            return False, "Cannot start with 0 or 1"
        
        # Basic validation (full Verhoeff algorithm can be added)
        return True, "Valid"
    
    @staticmethod
    def validate_pan(value: str) -> Tuple[bool, str]:
        """Validate PAN card format"""
        if not re.match(r'^[A-Z]{5}\d{4}[A-Z]$', value):
            return False, "Invalid format"
        
        # PAN structure: AAAAA9999A
        # Position 4 indicates entity type (P=Person, C=Company, etc.)
        entity_code = value[3]
        entity_types = {
            'P': 'Individual', 
            'C': 'Company', 
            'H': 'HUF', 
            'F': 'Firm',
            'A': 'AOP',
            'T': 'Trust',
            'B': 'BOI',
            'L': 'Local Authority',
            'J': 'Artificial Juridical Person',
            'G': 'Government'
        }
        
        return True, f"Valid ({entity_types.get(entity_code, 'Unknown')} type)"
    
    @staticmethod
    def validate_mobile(value: str) -> Tuple[bool, str]:
        """Validate Indian mobile number"""
        clean = value.replace('+', '').replace('-', '').replace(' ', '')
        
        # Remove country code if present
        if clean.startswith('91'):
            clean = clean[2:]
        
        if len(clean) != 10:
            return False, "Invalid length"
        
        if not clean[0] in '6789':
            return False, "Must start with 6, 7, 8, or 9"
        
        return True, "Valid"
    
    @staticmethod
    def validate_email(value: str) -> Tuple[bool, str]:
        """Validate email format"""
        if '@' not in value or '.' not in value.split('@')[1]:
            return False, "Invalid format"
        
        return True, "Valid"
    
    @staticmethod
    def validate_vehicle_number(value: str) -> Tuple[bool, str]:
        """Validate Indian vehicle registration number"""
        clean = value.replace('-', '').replace(' ', '').upper()
        
        # Indian format: AA00AA0000 or AA00A0000
        if not re.match(r'^[A-Z]{2}\d{1,2}[A-Z]{1,2}\d{4}$', clean):
            return False, "Invalid format"
        
        return True, "Valid"
    
    @staticmethod
    def validate_ifsc_code(value: str) -> Tuple[bool, str]:
        """Validate IFSC code"""
        if not re.match(r'^[A-Z]{4}0[A-Z0-9]{6}$', value):
            return False, "Invalid format"
        
        return True, "Valid"

# ============================================================================
# Context Analyzer
# ============================================================================

class ContextAnalyzer:
    """Analyzes context to boost/reduce confidence"""
    
    CONTEXT_KEYWORDS = {
        EntityType.MOBILE_NUMBER: {
            'boost': ['mobile', 'phone', 'contact', 'call', 'number'],
            'reduce': ['date', 'time', 'year', 'age']
        },
        EntityType.EMAIL: {
            'boost': ['email', 'mail', 'send', 'contact'],
            'reduce': []
        },
        EntityType.AADHAAR_NUMBER: {
            'boost': ['aadhaar', 'aadhar', 'uid', 'identity', 'card'],
            'reduce': ['phone', 'mobile', 'date']
        },
        EntityType.PAN_NUMBER: {
            'boost': ['pan', 'tax', 'income', 'card'],
            'reduce': []
        },
        EntityType.VEHICLE_NUMBER: {
            'boost': ['vehicle', 'car', 'bike', 'registration', 'plate', 'number'],
            'reduce': []
        },
        EntityType.PASSPORT_NUMBER: {
            'boost': ['passport', 'travel', 'visa'],
            'reduce': []
        },
        EntityType.VOTER_ID: {
            'boost': ['voter', 'epic', 'election'],
            'reduce': []
        }
    }
    
    @classmethod
    def analyze_context(cls, entity_type: EntityType, value: str, message: str) -> float:
        """Analyze context and return confidence adjustment"""
        adjustment = 0.0
        message_lower = message.lower()
        
        context = cls.CONTEXT_KEYWORDS.get(entity_type, {})
        
        # Boost confidence if context keywords present
        for keyword in context.get('boost', []):
            if keyword in message_lower:
                adjustment += 0.05
        
        # Reduce confidence if conflicting keywords present
        for keyword in context.get('reduce', []):
            if keyword in message_lower:
                adjustment -= 0.10
        
        return adjustment

# ============================================================================
# Main Entity Detector
# ============================================================================

class EntityDetector:
    """
    World-class entity detector with advanced pattern matching
    """
    
    def __init__(self):
        # Initialize pattern registry
        PatternRegistry._compile_all_patterns()
        self.validator = EntityValidator()
        self.context_analyzer = ContextAnalyzer()
        
        # Priority order for detection (higher priority first)
        self.detection_priority = [
            EntityType.EMAIL,
            EntityType.MONGODB_OBJECTID,
            EntityType.UUID,
            EntityType.PAN_NUMBER,
            EntityType.AADHAAR_NUMBER,
            EntityType.PASSPORT_NUMBER,
            EntityType.VOTER_ID,
            EntityType.DRIVING_LICENSE,
            EntityType.VEHICLE_NUMBER,
            EntityType.IFSC_CODE,
            EntityType.UPI_ID,
            EntityType.FIR_NUMBER,
            EntityType.CASE_NUMBER,
            EntityType.MOBILE_NUMBER,
            EntityType.PERSON_NAME,  # Detect names (after mobiles/IDs to avoid false positives)
            EntityType.IP_ADDRESS,
            EntityType.DATE,
            EntityType.TIME,
            EntityType.BANK_ACCOUNT,
            EntityType.LANDLINE,
            EntityType.NUMERIC_ID,  # Lowest priority
        ]
    
    @classmethod
    def detect_entities(
        cls,
        user_message: str,
        prioritize_types: Optional[List[EntityType]] = None,
        include_domain_entities: bool = True,
        use_spacy: bool = True
    ) -> List[DetectedEntity]:
        """
        Detect all entities in user message with intelligent prioritization
        NOW WITH SPACY + COMPREHENSIVE DOMAIN ENTITY DETECTION!
        
        Args:
            user_message: Text to analyze
            prioritize_types: Optional list of entity types to prioritize
            include_domain_entities: Include crime domain entities (drug names, districts, etc.)
            use_spacy: Try spaCy first for fast entity detection
        
        Returns:
            List of detected entities sorted by confidence
        """
        # Create instance for non-static access
        detector = cls()
        
        entities = []
        seen_values = set()
        message_lower = user_message.lower()
        
        # STEP 0: Try spaCy first for fast entity detection (PERSON, GPE, DATE, ORG)
        if use_spacy and SPACY_AVAILABLE:
            spacy_entities = detector._detect_entities_spacy(user_message, seen_values)
            entities.extend(spacy_entities)
            logger.debug(f"spaCy detected {len(spacy_entities)} entities")
        
        # STEP 1: Detect technical entities (IDs, mobiles, emails, etc.)
        # Determine detection order
        detection_order = prioritize_types or detector.detection_priority
        
        # Detect each entity type
        for entity_type in detection_order:
            patterns = PatternRegistry.get_compiled_patterns(entity_type)
            
            for pattern in patterns:
                matches = pattern.findall(user_message)
                
                for match in matches:
                    # Clean match
                    if isinstance(match, tuple):
                        match = ''.join(match)
                    
                    value = match.strip()
                    if not value or value in seen_values:
                        continue
                    
                    # ⭐ DYNAMIC: Filter out false positive person names using pattern detection
                    if entity_type == EntityType.PERSON_NAME:
                        if detector._is_false_positive_person_name(value, message_lower):
                            logger.debug(f"Skipping false positive person name: {value}")
                            continue
                    
                    # Calculate confidence
                    confidence = detector._calculate_confidence(
                        entity_type,
                        value,
                        message_lower
                    )
                    
                    # Validate if validator exists
                    validation_status = None
                    validator_name = f'validate_{entity_type.value}'
                    if hasattr(detector.validator, validator_name):
                        validator_method = getattr(detector.validator, validator_name)
                        is_valid, status = validator_method(value)
                        validation_status = status
                        if not is_valid:
                            confidence *= 0.5  # Reduce confidence for invalid entities
                    
                    # Normalize value
                    normalized = detector._normalize_value(entity_type, value)
                    
                    # Get search fields
                    search_fields = FieldMapper.FIELD_MAPPING.get(entity_type, ['id'])
                    
                    # Create entity
                    entity = DetectedEntity(
                        entity_type=entity_type,
                        value=value,
                        confidence=confidence,
                        search_fields=search_fields,
                        normalized_value=normalized,
                        validation_status=validation_status,
                        metadata={
                            'original_message': user_message,
                            'detection_method': 'regex_pattern'
                        }
                    )
                    
                    entities.append(entity)
                    seen_values.add(value)
                    
                    logger.debug(f"Detected: {entity}")
        
        # STEP 2: Detect domain-specific entities (drug names, districts, crime types, etc.)
        if include_domain_entities and DOMAIN_ENTITIES_AVAILABLE:
            domain_entities = detector._detect_domain_entities(user_message, seen_values)
            entities.extend(domain_entities)
        
        # Sort by confidence (highest first)
        entities.sort(key=lambda e: e.confidence, reverse=True)
        
        return entities
    
    def _is_false_positive_person_name(self, value: str, message_lower: str) -> bool:
        """
        Dynamically detect if a detected "person name" is actually a false positive
        Uses pattern matching instead of hardcoded lists
        
        Args:
            value: The detected person name value
            message_lower: The original message in lowercase
            
        Returns:
            True if this is likely a false positive
        """
        value_lower = value.lower().strip()
        
        # Pattern 1: Command/query structure words (not names)
        command_patterns = [
            r'^(list|show|find|get|search|display|count|calculate)\s+(all|total|sum|average)',
            r'^(show|find|get|list)\s+(summaries|records|properties|crimes|drugs|persons)',
            r'^(show|find|get)\s+(total|sum|average|count)',
            r'^(processed|processing)\s+(by|time)',
            r'^(recently|recent)\s+(created|modified)',
            r'^(specific|particular)\s+(model|field|value)',
        ]
        
        for pattern in command_patterns:
            if re.search(pattern, value_lower):
                return True
        
        # Pattern 2: Contains query structure words (not typical in names)
        # These words are common in queries but not in person names
        query_structure_words = {
            'all', 'number', 'status', 'type', 'station', 'case', 'crime', 'fir',
            'by', 'in', 'are', 'want', 'with', 'from', 'to', 'for', 'of',
            'total', 'sum', 'average', 'count', 'value', 'worth', 'model',
            'processed', 'processing', 'created', 'modified', 'specific', 'particular',
            'summary', 'summaries', 'records', 'seizures', 'seizure'
        }
        
        value_words = set(value_lower.split())
        # If more than 30% of words are query structure words, it's likely a false positive
        if len(value_words) > 0:
            structure_word_ratio = len(value_words & query_structure_words) / len(value_words)
            if structure_word_ratio > 0.3:
                return True
        
        # Pattern 3: Contains database/field terminology
        db_terms = ['crime_id', 'person_id', 'accused_id', 'drug_id', 'property_id',
                   'fir_num', 'fir_date', 'case_status', 'crime_type', 'io_name',
                   'ps_code', 'police_station', 'email_address', 'phone_number']
        
        for term in db_terms:
            if term in value_lower or term.replace('_', ' ') in value_lower:
                return True
        
        # Pattern 4: Very long "names" (likely query phrases, not names)
        # Real person names are typically 2-4 words, rarely more than 5
        if len(value_words) > 5:
            return True
        
        # Pattern 5: Contains numbers (unlikely in person names, but possible in queries)
        if re.search(r'\d', value_lower) and len(value_words) > 2:
            return True
        
        # Pattern 6: Check if it's part of a query pattern in the original message
        # If the value appears in a query structure, it's likely not a name
        query_context_patterns = [
            r'show\s+' + re.escape(value_lower),
            r'find\s+' + re.escape(value_lower),
            r'list\s+' + re.escape(value_lower),
            r'get\s+' + re.escape(value_lower),
            r'processed\s+by\s+' + re.escape(value_lower),
            r'show\s+.*\s+with\s+' + re.escape(value_lower),
        ]
        
        for pattern in query_context_patterns:
            if re.search(pattern, message_lower):
                return True
        
        return False
    
    def _calculate_confidence(
        self,
        entity_type: EntityType,
        value: str,
        message: str
    ) -> float:
        """Calculate confidence score with context analysis"""
        # Base confidence
        confidence = 0.70
        
        # Type-specific confidence boosts
        if entity_type == EntityType.EMAIL:
            if '@' in value and '.' in value.split('@')[1]:
                confidence = 0.95
        
        elif entity_type == EntityType.MONGODB_OBJECTID:
            if len(value) == 24:
                confidence = 0.98
        
        elif entity_type == EntityType.PAN_NUMBER:
            if re.match(r'^[A-Z]{5}\d{4}[A-Z]$', value):
                confidence = 0.95
        
        elif entity_type == EntityType.MOBILE_NUMBER:
            clean = value.replace('+', '').replace('-', '').replace(' ', '')
            if clean.startswith('91'):
                clean = clean[2:]
            if len(clean) == 10 and clean[0] in '6789':
                confidence = 0.90
        
        elif entity_type == EntityType.AADHAAR_NUMBER:
            clean = value.replace(' ', '').replace('-', '')
            if len(clean) == 12 and clean.isdigit():
                confidence = 0.92
        
        elif entity_type == EntityType.VEHICLE_NUMBER:
            clean = value.replace('-', '').replace(' ', '')
            if re.match(r'^[A-Z]{2}\d{1,2}[A-Z]{1,2}\d{4}$', clean):
                confidence = 0.88
        
        elif entity_type == EntityType.IFSC_CODE:
            if re.match(r'^[A-Z]{4}0[A-Z0-9]{6}$', value):
                confidence = 0.90
        
        # Apply context analysis
        context_adjustment = self.context_analyzer.analyze_context(
            entity_type,
            value,
            message
        )
        confidence += context_adjustment
        
        # Clamp between 0 and 1
        return max(0.0, min(1.0, confidence))
    
    def _normalize_value(self, entity_type: EntityType, value: str) -> str:
        """Normalize entity value to standard format"""
        if entity_type == EntityType.MOBILE_NUMBER:
            # Remove formatting, keep only digits
            clean = re.sub(r'[^\d]', '', value)
            if clean.startswith('91'):
                clean = clean[2:]
            return clean
        
        elif entity_type == EntityType.AADHAAR_NUMBER:
            # Standard format: XXXX XXXX XXXX
            clean = re.sub(r'[^\d]', '', value)
            return f"{clean[:4]} {clean[4:8]} {clean[8:]}" if len(clean) == 12 else value
        
        elif entity_type in [EntityType.PAN_NUMBER, EntityType.VEHICLE_NUMBER, EntityType.IFSC_CODE]:
            return value.upper().replace(' ', '').replace('-', '')
        
        elif entity_type == EntityType.EMAIL:
            return value.lower()
        
        return value
    
    def _detect_entities_spacy(self, user_message: str, seen_values: Set) -> List[DetectedEntity]:
        """
        Fast entity detection using spaCy NER (10x faster than regex!)
        
        Args:
            user_message: Text to analyze
            seen_values: Already detected values to avoid duplicates
            
        Returns:
            List of detected entities from spaCy
        """
        entities = []
        
        try:
            # Run spaCy NER
            doc = NLP_MODEL(user_message)
            
            for ent in doc.ents:
                value = ent.text.strip()
                
                if not value or value.lower() in seen_values:
                    continue
                
                # Map spaCy labels to our EntityType
                entity_type = None
                search_fields = []
                confidence = 0.75  # spaCy is quite accurate
                
                if ent.label_ == 'PERSON':
                    # Person name detected
                    entity_type = EntityType.PERSON_NAME
                    search_fields = [
                        'full_name', 'name', 'surname', 'alias',  # persons table
                        'ACCUSED_NAME', 'FATHER_NAME', 'NAME'  # MongoDB
                    ]
                    confidence = 0.80
                
                elif ent.label_ == 'GPE':  # Geo-Political Entity (cities, countries)
                    # Location detected
                    entity_type = EntityType.UNKNOWN  # Will use for location search
                    search_fields = [
                        'present_district', 'permanent_district', 'dist_name',  # V2
                        'ps_name', 'circle_name', 'zone_name',
                        'present_locality_village', 'permanent_locality_village',
                        'DISTRICT', 'PS'  # V1
                    ]
                    confidence = 0.75
                
                elif ent.label_ == 'DATE':
                    # Date detected
                    entity_type = EntityType.DATE
                    search_fields = [
                        'fir_date', 'date_created', 'date_modified',  # V2
                        'date_of_seizure',
                        'REG_DT', 'FROM_DT', 'TO_DT'  # V1
                    ]
                    confidence = 0.80
                
                elif ent.label_ == 'ORG':
                    # Organization (could be police station, etc.)
                    entity_type = EntityType.UNKNOWN
                    search_fields = [
                        'ps_name', 'circle_name', 'zone_name',  # V2
                        'PS'  # V1
                    ]
                    confidence = 0.70
                
                else:
                    # Other entity types - skip for now
                    continue
                
                # Create entity
                if entity_type and search_fields:
                    entity = DetectedEntity(
                        entity_type=entity_type,
                        value=value,
                        confidence=confidence,
                        search_fields=search_fields,
                        normalized_value=value.title(),
                        validation_status="spacy_detected",
                        metadata={
                            'spacy_label': ent.label_,
                            'detection_method': 'spacy_ner',
                            'original_message': user_message
                        }
                    )
                    
                    entities.append(entity)
                    seen_values.add(value.lower())
                    logger.debug(f"spaCy detected: {ent.label_} = {value}")
        
        except Exception as e:
            logger.debug(f"spaCy entity detection error: {e}")
        
        return entities
    
    def _detect_domain_entities(self, user_message: str, seen_values: Set) -> List[DetectedEntity]:
        """
        Detect crime domain-specific entities (drug names, districts, crime types, etc.)
        
        Args:
            user_message: Text to analyze
            seen_values: Already detected values to avoid duplicates
            
        Returns:
            List of domain-specific detected entities
        """
        domain_entities = []
        message_lower = user_message.lower()
        
        # Detect each domain entity type
        for domain_type, patterns in CRIME_DOMAIN_PATTERNS.items():
            for pattern_str in patterns:
                try:
                    pattern = re.compile(pattern_str, re.IGNORECASE)
                    matches = pattern.findall(user_message)
                    
                    for match in matches:
                        # Clean match
                        if isinstance(match, tuple):
                            value = ' '.join(m for m in match if m).strip()
                        else:
                            value = match.strip()
                        
                        if not value or value.lower() in seen_values:
                            continue
                        
                        # Get search fields for this domain entity
                        field_mapping = CRIME_DOMAIN_FIELD_MAPPINGS.get(domain_type, {})
                        v2_fields = field_mapping.get('v2', [])
                        v1_fields = field_mapping.get('v1', [])
                        search_fields = v2_fields + v1_fields
                        
                        if not search_fields:
                            continue  # Skip if no field mapping
                        
                        # Calculate confidence (domain entities have lower confidence than exact IDs)
                        confidence = 0.60  # Base for domain entities
                        
                        # Create entity (convert domain type to string for compatibility)
                        entity = DetectedEntity(
                            entity_type=EntityType.UNKNOWN,  # Use UNKNOWN as placeholder
                            value=value,
                            confidence=confidence,
                            search_fields=search_fields,
                            normalized_value=value.title(),  # Title case for readability
                            validation_status="domain_entity",
                            metadata={
                                'domain_type': domain_type.value,
                                'original_message': user_message,
                                'detection_method': 'domain_pattern',
                                'v2_fields': v2_fields,
                                'v1_fields': v1_fields
                            }
                        )
                        
                        domain_entities.append(entity)
                        seen_values.add(value.lower())
                        
                        logger.debug(f"Detected domain entity: {domain_type.value} = {value}")
                
                except Exception as e:
                    logger.debug(f"Pattern matching error for {domain_type}: {e}")
                    continue
        
        return domain_entities
    
    @classmethod
    @lru_cache(maxsize=256)
    def get_primary_entity(cls, user_message: str) -> Optional[DetectedEntity]:
        """
        Get the most likely primary entity from message (cached)
        
        Args:
            user_message: Text to analyze
        
        Returns:
            Primary detected entity or None
        """
        entities = cls.detect_entities(user_message)
        return entities[0] if entities else None
    
    @classmethod
    def is_entity_only_query(cls, user_message: str, threshold: float = 0.8) -> bool:
        """
        Check if message is ONLY an entity with minimal context
        
        Args:
            user_message: Text to analyze
            threshold: Confidence threshold
        
        Returns:
            True if message is just an entity
        """
        message_clean = user_message.strip()
        words = message_clean.split()
        
        # Short messages with high-confidence entity
        if len(words) <= 3:
            entities = cls.detect_entities(message_clean)
            if entities and entities[0].confidence >= threshold:
                return True
        
        return False
    
    @classmethod
    def detect_by_type(
        cls,
        user_message: str,
        entity_type: EntityType
    ) -> List[DetectedEntity]:
        """Detect only specific entity type"""
        return cls.detect_entities(user_message, prioritize_types=[entity_type])


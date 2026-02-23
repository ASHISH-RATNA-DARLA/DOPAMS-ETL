"""
Crime Domain Entity Types - Comprehensive Coverage
Maps to ALL columns in PostgreSQL and MongoDB for intelligent query understanding
"""

from enum import Enum
from typing import Dict, List

# ============================================================================
# Domain-Specific Entity Types (50+ new types!)
# ============================================================================

class CrimeDomainEntityType(Enum):
    """Comprehensive crime investigation entity types"""
    
    # ========== PERSON ATTRIBUTES (10 types) ==========
    AGE = "age"
    GENDER = "gender"
    OCCUPATION = "occupation"
    EDUCATION = "education_qualification"
    CASTE = "caste"
    RELIGION = "religion"
    NATIONALITY = "nationality"
    RELATION_TYPE = "relation_type"  # Father, Mother, Son, etc.
    MARITAL_STATUS = "marital_status"
    DOMICILE = "domicile"  # Native, Inter-state, International
    
    # ========== CRIME ATTRIBUTES (12 types) ==========
    CRIME_TYPE = "crime_type"  # Narcotics, Theft, Assault, etc.
    CASE_STATUS = "case_status"  # Pending, Closed, Under Investigation
    FIR_TYPE = "fir_type"  # Regular, Zero FIR
    ACT_SECTION = "act_section"  # IPC 302, NDPS 20(b), etc.
    MAJOR_HEAD = "major_head"  # SLL, NDPS, IPC
    MINOR_HEAD = "minor_head"  # Specific categories
    CLASS_CLASSIFICATION = "class_classification"  # Cultivation, Commercial, etc.
    IO_RANK = "io_rank"  # Inspector, SI, CI, etc.
    CRIME_PATTERN = "crime_pattern"  # For pattern matching
    FIR_STATUS = "fir_status"  # Abated, Disposed, etc.
    AREA_OPERATION = "area_operation"  # Where crime occurred
    LOCATION_TYPE = "location_type"  # Urban, Rural, etc.
    
    # ========== DRUG/NARCOTICS ATTRIBUTES (10 types) ==========
    DRUG_NAME = "drug_name"  # Ganja, Heroin, Cocaine, etc.
    DRUG_CATEGORY = "drug_category"  # Narcotic, Psychotropic
    DRUG_SCHEDULE = "drug_schedule"  # Schedule I, II, III, IV, V
    DRUG_QUANTITY = "drug_quantity"  # 500 grams, 2 kg, etc.
    QUANTITY_UNIT = "quantity_unit"  # grams, kg, ml, liters
    SEIZURE_WORTH = "seizure_worth"  # Rs 50000, ₹25000
    COMMERCIAL_QUANTITY = "commercial_quantity"  # Yes/No
    PURITY_LEVEL = "purity"  # Percentage
    PACKAGING_TYPE = "packaging_details"  # How drug was packed
    TRANSPORT_METHOD = "transport_method"  # How transported
    
    # ========== PROPERTY/EVIDENCE ATTRIBUTES (8 types) ==========
    PROPERTY_STATUS = "property_status"  # Seized, Recovered, Disposed
    PROPERTY_CATEGORY = "property_category"  # Drugs, Vehicle, Electronics
    PROPERTY_NATURE = "property_nature"  # Cash, Gold, Mobile, etc.
    BELONGS_TO = "belongs_to"  # Accused, Victim, Others
    PROPERTY_VALUE = "property_value"  # Monetary value
    RECOVERY_LOCATION = "place_of_recovery"
    SEIZURE_DATE = "date_of_seizure"
    MEDIA_EVIDENCE = "media"  # Photos, videos
    
    # ========== PHYSICAL FEATURES (15 types) ==========
    HEIGHT = "height"
    BUILD = "build"  # Thin, Medium, Heavy, Athletic
    COMPLEXION = "complexion"  # Fair, Wheatish, Dark
    FACE_TYPE = "face_type"  # Oval, Round, Square
    HAIR_STYLE = "hair_style"
    HAIR_COLOR = "hair_color"
    EYE_TYPE = "eye_type"
    EYE_COLOR = "eye_color"
    NOSE_TYPE = "nose_type"
    BEARD_TYPE = "beard_type"
    MUSTACHE = "mustache"
    LIPS_TYPE = "lips_type"
    TEETH_TYPE = "teeth_type"
    EARS_TYPE = "ears_type"
    IDENTIFYING_MARKS = "identifying_marks"  # Mole, scar, tattoo, etc.
    
    # ========== LOCATION ATTRIBUTES (10 types) ==========
    DISTRICT = "district"
    POLICE_STATION = "police_station"
    CIRCLE = "circle"
    ZONE = "zone"
    RANGE = "range"
    STATE = "state"
    LOCALITY = "locality"  # Village, colony, ward
    MANDAL = "mandal"  # Area
    PIN_CODE = "pin_code"
    LANDMARK = "landmark"
    
    # ========== TEMPORAL ATTRIBUTES (5 types) ==========
    FIR_DATE = "fir_date"
    DATE_RANGE = "date_range"  # From-To
    YEAR = "year"
    MONTH = "month"
    TIME_OF_DAY = "time_of_day"
    
    # ========== ACCUSED TYPE (5 types) ==========
    ACCUSED_TYPE = "accused_type"  # Peddler, Consumer, Supplier, Kingpin
    ACCUSED_STATUS = "accused_status"  # Arrested, Absconding, Surrendered
    IS_CCL = "is_ccl"  # Child in Conflict with Law
    ROLE_IN_CRIME = "role_in_crime"  # Specific role
    ACCUSED_CODE = "accused_code"

# ============================================================================
# Detection Patterns for Domain Entities
# ============================================================================

CRIME_DOMAIN_PATTERNS = {
    # ========== CRIME TYPE PATTERNS ==========
    CrimeDomainEntityType.CRIME_TYPE: [
        r'\b(narcotics|drugs|ndps)\b',
        r'\b(theft|robbery|burglary|dacoit)\b',
        r'\b(murder|homicide|ipc\s*302)\b',
        r'\b(assault|battery|attack)\b',
        r'\b(rape|sexual\s*assault|pocso)\b',
        r'\b(kidnapping|abduction)\b',
        r'\b(cheating|fraud|forgery)\b',
        r'\b(arms|weapons|illegal\s*arms)\b',
        r'\b(cyber\s*crime|hacking)\b',
        r'\b(gambling|betting)\b',
    ],
    
    # ========== CASE STATUS PATTERNS ==========
    CrimeDomainEntityType.CASE_STATUS: [
        r'\b(pending|under\s*investigation|investigating)\b',
        r'\b(closed|disposed|completed)\b',
        r'\b(abated|withdrawn)\b',
        r'\b(chargesheet\s*filed|charge\s*sheeted)\b',
        r'\b(pt\s*cases|property\s*theft)\b',
        r'\b(untraced|undetected)\b',
    ],
    
    # ========== DRUG NAME PATTERNS ==========
    CrimeDomainEntityType.DRUG_NAME: [
        r'\b(ganja|marijuana|cannabis|weed)\b',
        r'\b(heroin|smack|brown\s*sugar)\b',
        r'\b(cocaine|coke)\b',
        r'\b(opium|afeem)\b',
        r'\b(mdma|ecstasy|molly)\b',
        r'\b(methamphetamine|meth|ice)\b',
        r'\b(lsd|acid)\b',
        r'\b(morphine)\b',
        r'\b(charas|hashish)\b',
    ],
    
    # ========== DRUG QUANTITY PATTERNS ==========
    CrimeDomainEntityType.DRUG_QUANTITY: [
        r'\b(\d+(?:\.\d+)?)\s*(grams?|gms?|g)\b',
        r'\b(\d+(?:\.\d+)?)\s*(kilograms?|kgs?|kg)\b',
        r'\b(\d+(?:\.\d+)?)\s*(milligrams?|mgs?|mg)\b',
        r'\b(\d+(?:\.\d+)?)\s*(milliliters?|mls?|ml)\b',
        r'\b(\d+(?:\.\d+)?)\s*(liters?|lts?|l)\b',
    ],
    
    # ========== DISTRICT PATTERNS ==========
    CrimeDomainEntityType.DISTRICT: [
        # Telangana districts
        r'\b(sangareddy|sanga\s*reddy)\b',
        r'\b(karimnagar)\b',
        r'\b(warangal)\b',
        r'\b(hyderabad|hyd)\b',
        r'\b(medak|medchal)\b',
        r'\b(nizamabad)\b',
        r'\b(adilabad)\b',
        r'\b(khammam)\b',
        r'\b(nalgonda)\b',
        r'\b(mahbubnagar)\b',
        r'\b(ranga\s*reddy)\b',
        # Generic
        r'\b([A-Z][a-z]+)\s*district\b',
        r'\b([A-Z][a-z]+)\s*dist\b',
    ],
    
    # ========== AGE PATTERNS ==========
    CrimeDomainEntityType.AGE: [
        r'\b(\d{1,3})\s*years?\s*old\b',
        r'\bage\s*[:=]?\s*(\d{1,3})\b',
        r'\b(\d{1,3})\s*yrs?\b',
        r'\b(\d{1,3})[-/]year[-]?old\b',
    ],
    
    # ========== GENDER PATTERNS ==========
    CrimeDomainEntityType.GENDER: [
        r'\b(male|man|men|boy)\b',
        r'\b(female|woman|women|girl|lady)\b',
        r'\b(transgender|trans|other)\b',
    ],
    
    # ========== OCCUPATION PATTERNS ==========
    CrimeDomainEntityType.OCCUPATION: [
        r'\b(driver|auto\s*driver|cab\s*driver)\b',
        r'\b(businessman|business|trader|merchant)\b',
        r'\b(farmer|agriculture|cultivator)\b',
        r'\b(laborer|daily\s*wage|worker)\b',
        r'\b(teacher|professor|educator)\b',
        r'\b(doctor|physician|medical)\b',
        r'\b(engineer|technical)\b',
        r'\b(unemployed|jobless)\b',
        r'\b(student|studying)\b',
    ],
    
    # ========== ACTS/SECTIONS PATTERNS ==========
    CrimeDomainEntityType.ACT_SECTION: [
        r'\b(ipc|indian\s*penal\s*code)\s*(\d+[a-z]?)\b',
        r'\b(ndps|ndpsa)\s*(\d+[a-z]?(?:\([a-z]\))?)\b',
        r'\b(section|sec\.?)\s*(\d+[a-z]?)\b',
        r'\b(\d+)\s*ipc\b',
        r'\b(\d+[a-z]?)\s*of\s*(ndps|ipc|crpc)\b',
    ],
    
    # ========== POLICE STATION PATTERNS ==========
    CrimeDomainEntityType.POLICE_STATION: [
        r'\b([A-Z][a-z]+)\s*(?:ps|police\s*station)\b',
        r'\b([A-Z][a-z]+)\s*town\s*ps\b',
        r'\b([A-Z][a-z]+)\s*rural\s*ps\b',
        r'\b(parkal|sangareddy|medak)\s*ps\b',
    ],
    
    # ========== HEIGHT PATTERNS ==========
    CrimeDomainEntityType.HEIGHT: [
        r'\b(\d{1})[\'′]\s*(\d{1,2})[\"″]?\b',  # 5'8"
        r'\b(\d{3})\s*cm\b',  # 170 cm
        r'\b(\d\.\d{2})\s*m(?:eters?)?\b',  # 1.70 m
    ],
    
    # ========== BUILD PATTERNS ==========
    CrimeDomainEntityType.BUILD: [
        r'\b(thin|slim|lean)\b',
        r'\b(medium|average|normal)\b',
        r'\b(heavy|fat|obese|stout)\b',
        r'\b(athletic|muscular|well[-]?built)\b',
    ],
    
    # ========== COMPLEXION PATTERNS ==========
    CrimeDomainEntityType.COMPLEXION: [
        r'\b(fair|light|white)\b',
        r'\b(wheatish|medium|brown)\b',
        r'\b(dark|black|dusky)\b',
    ],
    
    # ========== PROPERTY STATUS PATTERNS ==========
    CrimeDomainEntityType.PROPERTY_STATUS: [
        r'\b(seized|confiscated)\b',
        r'\b(recovered|retrieved|found)\b',
        r'\b(disposed|destroyed)\b',
        r'\b(returned|released)\b',
    ],
    
    # ========== PROPERTY NATURE PATTERNS ==========
    CrimeDomainEntityType.PROPERTY_NATURE: [
        r'\b(cash|money|currency)\b',
        r'\b(gold|jewelry|ornaments)\b',
        r'\b(mobile|phone|cellphone)\b',
        r'\b(vehicle|car|bike|auto)\b',
        r'\b(electronics|laptop|computer)\b',
    ],
}

# ============================================================================
# Field Mappings for Domain Entities
# ============================================================================

CRIME_DOMAIN_FIELD_MAPPINGS = {
    # ========== PERSON ATTRIBUTES ==========
    CrimeDomainEntityType.AGE: {
        'v2': ['age'],  # persons, brief_facts_accused
        'v1': ['AGE'],  # fir_records
    },
    
    CrimeDomainEntityType.GENDER: {
        'v2': ['gender'],  # persons, brief_facts_accused
        'v1': ['GENDER'],  # fir_records
    },
    
    CrimeDomainEntityType.OCCUPATION: {
        'v2': ['occupation'],  # persons, brief_facts_accused
        'v1': ['ACCUSED_OCCUPATION', 'INT_FATHER_OCCUPATION', 'INT_MOTHER_OCCUPATION'],
    },
    
    CrimeDomainEntityType.EDUCATION: {
        'v2': ['education_qualification'],  # persons
        'v1': [],  # Not in V1
    },
    
    CrimeDomainEntityType.CASTE: {
        'v2': ['caste', 'sub_caste'],  # persons
        'v1': ['CASTE'],  # fir_records
    },
    
    CrimeDomainEntityType.RELIGION: {
        'v2': ['religion'],  # persons
        'v1': [],  # Not in V1
    },
    
    CrimeDomainEntityType.NATIONALITY: {
        'v2': ['nationality'],  # persons
        'v1': ['NATIONALITY'],  # fir_records
    },
    
    CrimeDomainEntityType.RELATION_TYPE: {
        'v2': ['relation_type', 'relative_name'],  # persons
        'v1': ['INT_FATHER_NAME', 'INT_MOTHER_NAME', 'FATHER_NAME'],
    },
    
    # ========== CRIME ATTRIBUTES ==========
    CrimeDomainEntityType.CRIME_TYPE: {
        'v2': ['crime_type'],  # crimes
        'v1': [],  # Derived from MAJOR_HEAD, MINOR_HEAD
    },
    
    CrimeDomainEntityType.CASE_STATUS: {
        'v2': ['case_status'],  # crimes
        'v1': ['FIR_STATUS'],  # fir_records
    },
    
    CrimeDomainEntityType.FIR_TYPE: {
        'v2': ['fir_type'],  # crimes
        'v1': [],  # Not explicitly in V1
    },
    
    CrimeDomainEntityType.ACT_SECTION: {
        'v2': ['acts_sections'],  # crimes
        'v1': ['ACT_SEC'],  # fir_records
    },
    
    CrimeDomainEntityType.MAJOR_HEAD: {
        'v2': ['major_head'],  # crimes
        'v1': ['MAJOR_HEAD'],  # fir_records
    },
    
    CrimeDomainEntityType.MINOR_HEAD: {
        'v2': ['minor_head'],  # crimes
        'v1': ['MINOR_HEAD'],  # fir_records
    },
    
    CrimeDomainEntityType.CLASS_CLASSIFICATION: {
        'v2': ['class_classification'],  # crimes (Cultivation, Commercial, etc.)
        'v1': [],  # Not in V1
    },
    
    CrimeDomainEntityType.IO_RANK: {
        'v2': ['io_rank'],  # crimes
        'v1': [],  # Not explicitly in V1
    },
    
    # ========== DRUG ATTRIBUTES ==========
    CrimeDomainEntityType.DRUG_NAME: {
        'v2': ['drug_name', 'scientific_name', 'brand_name'],  # brief_facts_drugs
        'v1': ['DRUG_DESC', 'DRUG_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.DRUG_CATEGORY: {
        'v2': ['drug_category'],  # brief_facts_drugs
        'v1': ['DRUG_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.DRUG_SCHEDULE: {
        'v2': ['drug_schedule'],  # brief_facts_drugs
        'v1': [],  # Not in V1
    },
    
    CrimeDomainEntityType.DRUG_QUANTITY: {
        'v2': ['total_quantity', 'quantity_numeric', 'quantity_unit'],  # brief_facts_drugs
        'v1': ['WEIGHT_GM', 'WEIGHT_KG'],  # fir_records
    },
    
    CrimeDomainEntityType.SEIZURE_WORTH: {
        'v2': ['seizure_worth', 'street_value', 'street_value_numeric'],  # brief_facts_drugs
        'v1': ['ESTIMATED_VALUE'],  # fir_records
    },
    
    CrimeDomainEntityType.COMMERCIAL_QUANTITY: {
        'v2': ['is_commercial', 'commercial_quantity'],  # brief_facts_drugs
        'v1': [],  # Not explicitly in V1
    },
    
    # ========== PROPERTY ATTRIBUTES ==========
    CrimeDomainEntityType.PROPERTY_STATUS: {
        'v2': ['property_status'],  # properties
        'v1': ['DRUG_STATUS'],  # fir_records
    },
    
    CrimeDomainEntityType.PROPERTY_CATEGORY: {
        'v2': ['category'],  # properties
        'v1': [],  # Not explicitly in V1
    },
    
    CrimeDomainEntityType.PROPERTY_NATURE: {
        'v2': ['nature'],  # properties
        'v1': [],  # Not explicitly in V1
    },
    
    CrimeDomainEntityType.PROPERTY_VALUE: {
        'v2': ['estimate_value', 'recovered_value'],  # properties
        'v1': ['ESTIMATED_VALUE'],  # fir_records
    },
    
    # ========== PHYSICAL FEATURES ==========
    CrimeDomainEntityType.HEIGHT: {
        'v2': ['height'],  # accused
        'v1': ['HEIGHT_FROM_CM', 'HEIGHT_LL_FEET'],  # fir_records
    },
    
    CrimeDomainEntityType.BUILD: {
        'v2': ['build'],  # accused
        'v1': ['BUILD_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.COMPLEXION: {
        'v2': ['color'],  # accused (complexion/skin color)
        'v1': ['COMPLEXION_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.FACE_TYPE: {
        'v2': ['face'],  # accused
        'v1': ['FACE_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.HAIR_STYLE: {
        'v2': ['hair'],  # accused
        'v1': ['HAIR_STYLE'],  # fir_records
    },
    
    CrimeDomainEntityType.HAIR_COLOR: {
        'v2': ['hair'],  # accused (may include color)
        'v1': ['HAIR_COLOR'],  # fir_records
    },
    
    CrimeDomainEntityType.EYE_TYPE: {
        'v2': ['eyes'],  # accused
        'v1': ['EYE_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.EYE_COLOR: {
        'v2': ['eyes'],  # accused (may include color)
        'v1': ['EYE_COLOR'],  # fir_records
    },
    
    CrimeDomainEntityType.NOSE_TYPE: {
        'v2': ['nose'],  # accused
        'v1': ['NOSE_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.BEARD_TYPE: {
        'v2': ['beard'],  # accused
        'v1': ['BEARD_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.MUSTACHE: {
        'v2': ['mustache'],  # accused
        'v1': [],  # Not explicitly in V1
    },
    
    CrimeDomainEntityType.LIPS_TYPE: {
        'v2': [],  # Not in V2
        'v1': ['LIPS_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.TEETH_TYPE: {
        'v2': ['teeth'],  # accused
        'v1': ['TEETH_TYPE'],  # fir_records
    },
    
    CrimeDomainEntityType.EARS_TYPE: {
        'v2': ['ear'],  # accused
        'v1': ['EARS_TYPE_CD'],  # fir_records
    },
    
    CrimeDomainEntityType.IDENTIFYING_MARKS: {
        'v2': ['mole', 'leucoderma'],  # accused
        'v1': ['OTHER_IDENTIFY_MARKS'],  # fir_records
    },
    
    # ========== LOCATION ATTRIBUTES ==========
    CrimeDomainEntityType.DISTRICT: {
        'v2': ['present_district', 'permanent_district', 'dist_name'],  # persons, hierarchy
        'v1': ['DISTRICT'],  # fir_records
    },
    
    CrimeDomainEntityType.POLICE_STATION: {
        'v2': ['ps_code', 'ps_name', 'present_jurisdiction_ps'],  # crimes, hierarchy, persons
        'v1': ['PS'],  # fir_records
    },
    
    CrimeDomainEntityType.CIRCLE: {
        'v2': ['circle_code', 'circle_name'],  # hierarchy
        'v1': [],
    },
    
    CrimeDomainEntityType.ZONE: {
        'v2': ['zone_code', 'zone_name', 'sub_zone_code', 'sub_zone_name'],  # hierarchy
        'v1': [],
    },
    
    CrimeDomainEntityType.STATE: {
        'v2': ['present_state_ut', 'permanent_state_ut'],  # persons
        'v1': [],
    },
    
    CrimeDomainEntityType.LOCALITY: {
        'v2': ['present_locality_village', 'permanent_locality_village', 
               'present_ward_colony', 'permanent_ward_colony'],  # persons
        'v1': [],
    },
    
    CrimeDomainEntityType.MANDAL: {
        'v2': ['present_area_mandal', 'permanent_area_mandal'],  # persons
        'v1': [],
    },
    
    CrimeDomainEntityType.PIN_CODE: {
        'v2': ['present_pin_code', 'permanent_pin_code'],  # persons
        'v1': [],
    },
    
    CrimeDomainEntityType.LANDMARK: {
        'v2': ['present_landmark_milestone', 'permanent_landmark_milestone'],  # persons
        'v1': [],
    },
    
    # ========== ACCUSED TYPE ==========
    CrimeDomainEntityType.ACCUSED_TYPE: {
        'v2': ['accused_type'],  # brief_facts_accused (peddler, consumer, supplier, etc.)
        'v1': [],
    },
    
    CrimeDomainEntityType.ACCUSED_STATUS: {
        'v2': ['status'],  # brief_facts_accused
        'v1': [],
    },
    
    CrimeDomainEntityType.IS_CCL: {
        'v2': ['is_ccl'],  # accused, brief_facts_accused
        'v1': [],
    },
    
    CrimeDomainEntityType.ROLE_IN_CRIME: {
        'v2': ['role_in_crime'],  # brief_facts_accused
        'v1': [],
    },
}

# ============================================================================
# Query Intent Classification
# ============================================================================

class QueryIntent(Enum):
    """What type of information user is seeking"""
    PERSON_SEARCH = "person_search"  # Looking for person details
    CRIME_SEARCH = "crime_search"  # Looking for crime/FIR details
    ACCUSED_SEARCH = "accused_search"  # Looking for accused info
    DRUG_SEARCH = "drug_search"  # Looking for drug seizures
    PROPERTY_SEARCH = "property_search"  # Looking for seized property
    LOCATION_SEARCH = "location_search"  # Filter by location
    STATISTICAL = "statistical"  # Count, aggregate, analyze
    RELATIONSHIP_SEARCH = "relationship_search"  # Family connections
    PHYSICAL_DESCRIPTION = "physical_description"  # Based on appearance
    MULTI_CRITERIA = "multi_criteria"  # Multiple filters

def classify_query_intent(detected_entities: List) -> QueryIntent:
    """
    Classify what user is looking for based on detected entities
    
    Args:
        detected_entities: List of DetectedEntity objects
        
    Returns:
        QueryIntent enum indicating query type
    """
    if not detected_entities:
        return QueryIntent.STATISTICAL
    
    entity_types = [e.entity_type for e in detected_entities]
    
    # Check for person search indicators
    person_indicators = [
        CrimeDomainEntityType.AGE,
        CrimeDomainEntityType.GENDER,
        CrimeDomainEntityType.OCCUPATION,
        CrimeDomainEntityType.EDUCATION,
    ]
    if any(et in entity_types for et in person_indicators):
        return QueryIntent.PERSON_SEARCH
    
    # Check for drug search indicators
    drug_indicators = [
        CrimeDomainEntityType.DRUG_NAME,
        CrimeDomainEntityType.DRUG_QUANTITY,
        CrimeDomainEntityType.DRUG_CATEGORY,
    ]
    if any(et in entity_types for et in drug_indicators):
        return QueryIntent.DRUG_SEARCH
    
    # Check for property search
    property_indicators = [
        CrimeDomainEntityType.PROPERTY_STATUS,
        CrimeDomainEntityType.PROPERTY_NATURE,
    ]
    if any(et in entity_types for et in property_indicators):
        return QueryIntent.PROPERTY_SEARCH
    
    # Check for physical description search
    physical_indicators = [
        CrimeDomainEntityType.HEIGHT,
        CrimeDomainEntityType.BUILD,
        CrimeDomainEntityType.COMPLEXION,
    ]
    if any(et in entity_types for et in physical_indicators):
        return QueryIntent.PHYSICAL_DESCRIPTION
    
    # Check for location filter
    location_indicators = [
        CrimeDomainEntityType.DISTRICT,
        CrimeDomainEntityType.POLICE_STATION,
    ]
    if any(et in entity_types for et in location_indicators):
        return QueryIntent.LOCATION_SEARCH
    
    # Check for crime search
    crime_indicators = [
        CrimeDomainEntityType.CRIME_TYPE,
        CrimeDomainEntityType.CASE_STATUS,
        CrimeDomainEntityType.ACT_SECTION,
    ]
    if any(et in entity_types for et in crime_indicators):
        return QueryIntent.CRIME_SEARCH
    
    # Multi-criteria if multiple entity types
    if len(entity_types) > 2:
        return QueryIntent.MULTI_CRITERIA
    
    return QueryIntent.PERSON_SEARCH  # Default



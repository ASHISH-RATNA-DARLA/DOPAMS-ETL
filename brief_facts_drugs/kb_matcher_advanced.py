# Drug KB Matching - Production-Grade NDPS-Compliant System
# Behaves like Senior NDPS Officer: Meticulous Rule-Based Processing

# File: brief_facts_drugs/kb_matcher_advanced.py

import logging
import re
from typing import Tuple, Optional, Dict, List
from difflib import SequenceMatcher
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1: NDPS DOMAIN KNOWLEDGE - BUILT-IN RULES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NDPSActSection(Enum):
    """NDPS Act sections for validation."""
    SECTION_8 = "8"      # Possession
    SECTION_20 = "20"    # Consumption/manufacture
    SECTION_21 = "21"    # Production
    SECTION_22 = "22"    # Commerce
    SECTION_25 = "25"    # Cultivation
    SECTION_29 = "29"    # Criminal conspiracy

# Tier 1: Definitive drug names (100% match)
NDPS_TIER1_DRUGS = {
    'ganja': {'standard': 'Ganja', 'category': 'Cannabis', 'aliases': ['dry ganja', 'wet ganja', 'ganaj', 'gandja']},
    'charas': {'standard': 'Charas', 'category': 'Cannabis', 'aliases': ['chras', 'charash', 'chakkals']},
    'hashish': {'standard': 'Hashish', 'category': 'Cannabis', 'aliases': ['hash', 'hashis']},
    'heroin': {'standard': 'Heroin', 'category': 'Opioid', 'aliases': ['smack', 'brown sugar', 'H', 'dope']},
    'cocaine': {'standard': 'Cocaine', 'category': 'Stimulant', 'aliases': ['coke', 'cock', 'snow']},
    'opium': {'standard': 'Opium', 'category': 'Opioid', 'aliases': ['apheem', 'opiem']},
    'mdma': {'standard': 'MDMA', 'category': 'Stimulant', 'aliases': ['ecstasy', 'xtc', 'molly']},
    'lsd': {'standard': 'LSD', 'category': 'Hallucinogen', 'aliases': ['lysergic acid', 'acid']},
}

# Tier 2: Regional/slang variations by zone (India context)
REGIONAL_SLANG = {
    'andhra': {
        'ganja': ['ganja tamaku', 'tamaku', 'hemp', 'cannabis leaf'],
        'charas': ['charash', 'res'],
        'cocaine': ['coke', 'powder'],
    },
    'maharashtra': {
        'ganja': ['ganja', 'bhang'],
        'charas': ['charas', 'black hash'],
        'opium': ['afeem', 'apheem'],
    },
    'punjab': {
        'ganja': ['ganja', 'bhang leaves'],
        'opium': ['afeem'],
        'heroin': ['smack', 'brown sugar'],
    },
}

# Commercial quantity thresholds (NDPS Act)
COMMERCIAL_QUANTITY_NDPS = {
    'ganja': 20.0,              # kg
    'charas': 1.0,              # kg
    'hashish': 1.0,             # kg
    'heroin': 0.250,            # kg (250g)
    'cocaine': 0.500,           # kg (500g)
    'opium': 2.5,               # kg
    'mdma': 0.050,              # kg (50g)
    'lsd': 100,                 # blots/units
}

# Validity checks for drug form + unit combinations
VALID_FORM_UNIT_COMBINATIONS = {
    'solid': {'kg', 'g', 'mg', 'gm', 'gms', 'grm', 'gram', 'grams'},
    'liquid': {'l', 'ml', 'ltr', 'ltrs', 'litre', 'liters'},
    'count': {'no', 'nos', 'tablet', 'tablets', 'pill', 'pills', 'blot', 'blots', 'plant', 'plants'},
    'powder': {'kg', 'g', 'mg'},
}

# Suspicious patterns (potential false positives)
SUSPICIOUS_PATTERNS = {
    'purchase_only': r'(purchased|bought|acquired|ordered)[\s\d\w]*(but|however|yet)',  # says purchase but not seized
    'no_seizure': r'(information about|heard|rumor|allegation|reported)',  # No actual seizure
    'customer_list': r'(sold to|supplied to|delivered to)\s*[A-Z][a-z]+\s*,',  # Customer names, not accessories
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2: PRODUCTION-GRADE KB MATCHER WITH EDGE CASES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@dataclass
class MatchResult:
    """KB matching result with full audit trail."""
    standard_name: str
    category_group: str
    raw_kb_name: str
    match_confidence: float          # 0.0-1.0
    match_type: str                 # exact_indexed, fuzzy_high, fuzzy_medium, etc.
    match_ratio: float              # Actual similarity 0.0-1.0
    is_commercial: Optional[bool] = None
    commercial_threshold: Optional[float] = None
    validation_warnings: List[str] = None
    ai_confidence_boost: float = 0.0  # Boost/discount to apply to LLM confidence
    audit_log: Dict = None
    
    def __post_init__(self):
        if self.validation_warnings is None:
            self.validation_warnings = []
        if self.audit_log is None:
            self.audit_log = {}


class DrugKBMatcherAdvanced:
    """
    Production-grade KB matcher with NDPS compliance, edge case handling,
    and audit trails. Behaves like senior NDPS officer: methodical, rule-based.
    """
    
    # Thresholds (fine-tuned for Indian drug seizure data)
    EXACT_MATCH_THRESHOLD = 0.95
    HIGH_CONFIDENCE_THRESHOLD = 0.82
    MEDIUM_CONFIDENCE_THRESHOLD = 0.72
    LOW_THRESHOLD = 0.60
    
    # Confidence adjustments
    CONFIDENCE_BOOST_EXACT = 0.15
    CONFIDENCE_BOOST_HIGH = 0.08
    CONFIDENCE_DISCOUNT_UNCERTAIN = 0.10
    CONFIDENCE_DISCOUNT_SUSPICIOUS = 0.25
    
    def __init__(self, drug_categories: List[dict]):
        self.drug_categories = drug_categories or []
        self._build_indices()
        logger.info(f"DrugKBMatcherAdvanced initialized with {len(drug_categories)} KB entries")
    
    def _build_indices(self):
        """Build fast lookup indices for KB matching."""
        self.kb_exact_index = {}      # Exact matches (normalized)
        self.kb_aliases = {}          # Alias → standard mapping
        self.kb_by_standard = {}      # Standard name index
        
        for entry in self.drug_categories:
            raw = str(entry.get('raw_name', '')).strip()
            standard = str(entry.get('standard_name', '')).strip()
            category = str(entry.get('category_group', '')).strip()
            
            if not raw or not standard:
                continue
            
            # Build exact index
            norm_raw = self._normalize(raw)
            norm_std = self._normalize(standard)
            
            self.kb_exact_index[norm_raw] = {
                'standard': standard,
                'category': category,
                'raw': raw,
            }
            self.kb_exact_index[norm_std] = {
                'standard': standard,
                'category': category,
                'raw': raw,
            }
            
            # Index by standard name
            if standard not in self.kb_by_standard:
                self.kb_by_standard[standard] = []
            self.kb_by_standard[standard].append({'raw': raw, 'category': category})
            
            # Add built-in aliases from TIER1 if exists
            tier1_key = standard.lower()
            if tier1_key in NDPS_TIER1_DRUGS:
                tier1_entry = NDPS_TIER1_DRUGS[tier1_key]
                for alias in tier1_entry.get('aliases', []):
                    norm_alias = self._normalize(alias)
                    self.kb_aliases[norm_alias] = {
                        'standard': standard,
                        'category': category,
                        'from_tier1': True,
                    }
        
        logger.debug(f"KB indices: {len(self.kb_exact_index)} exact, {len(self.kb_aliases)} aliases")
    
    def _normalize(self, text: str) -> str:
        """
        Production-grade normalization.
        Handles: case, whitespace, punctuation, Indian abbreviations.
        """
        if not text:
            return ""
        
        # Lowercase
        text = text.lower().strip()
        
        # Expand common Indian abbreviations
        abbreviations = {
            r'\bgm\b': 'gram',
            r'\bgms\b': 'grams',
            r'\bkg\b': 'kilogram',
            r'\bkgs\b': 'kilograms',
            r'\bml\b': 'milliliter',
            r'\bltr\b': 'litre',
            r'\bltrs\b': 'litres',
            r'\bno\b': 'number',
            r'\bnos\b': 'numbers',
            r'\bpcs\b': 'pieces',
            r'\bdryganja\b': 'dry ganja',
            r'\bwetganja\b': 'wet ganja',
        }
        
        for pattern, expansion in abbreviations.items():
            text = re.sub(pattern, expansion, text)
        
        # Remove punctuation
        text = re.sub(r'[.,;:!?\-()"\']', ' ', text)
        
        # Collapse multiple spaces
        text = ' '.join(text.split())
        
        # Remove trailing filler words
        words = text.split()
        while words and words[-1] in ('of', 'and', 'a', 'the', 'in', 'or'):
            words.pop()
        
        return ' '.join(words)
    
    def _check_form_unit_validity(self, drug_form: str, raw_unit: str) -> Tuple[bool, str]:
        """
        Edge case: Validate drug_form matches unit type.
        Senior officer would catch: "Liquid ganja in grams" (invalid).
        """
        if not drug_form or not raw_unit:
            return True, ""  # Can't validate, assume OK
        
        form_norm = self._normalize(drug_form)
        unit_norm = re.sub(r'[^a-z]', '', raw_unit.lower().strip())
        
        for form_category, valid_units in VALID_FORM_UNIT_COMBINATIONS.items():
            if form_category in form_norm:
                if unit_norm not in valid_units:
                    return False, f"Form '{drug_form}' incompatible with unit '{raw_unit}'"
        
        return True, ""
    
    def _check_quantity_sanity(self, quantity: float, drug_name: str, drug_form: str) -> Tuple[bool, str]:
        """
        Edge case: Quantity sanity check.
        Senior officer: "5000 kg marijuana? That's wrong, must be typo or calculation error."
        """
        if not quantity or quantity <= 0:
            return True, ""  # Zero/negative quantities handled separately
        
        warnings = []
        
        # Check for obvious outliers
        if quantity > 10000:
            warnings.append(f"Quantity {quantity} seems extremely large, possible data entry error")
        
        # Check if clearly exceeds India's max recorded seizure
        # (Typical max: 500kg for ganja, 50kg for heroin)
        typical_max = {
            'ganja': 500,
            'charas': 50,
            'heroin': 50,
            'cocaine': 50,
        }
        
        drug_norm = self._normalize(drug_name)
        for drug_key, max_qty in typical_max.items():
            if drug_key in drug_norm and quantity > max_qty:
                warnings.append(f"Quantity {quantity} exceeds typical max {max_qty} for {drug_key}")
                break
        
        return len(warnings) == 0, "; ".join(warnings) if warnings else ""
    
    def _detect_false_positives(self, extracted_text: str, drug_name: str) -> Tuple[bool, str]:
        """
        Edge case: Detect extraction errors (rules R10 from prompt).
        Senior officer: "Is this drug actually SEIZED or just mentioned in context?"
        """
        # Check if this looks like customer list (not accused)
        if re.search(SUSPICIOUS_PATTERNS['customer_list'], extracted_text):
            if any(word in drug_name.lower() for word in ['customer', 'buyer', 'purchaser']):
                return False, "Appears to be customer list, not accused with seizure"
        
        # Check if only mentioned but not seized
        if re.search(SUSPICIOUS_PATTERNS['no_seizure'], extracted_text):
            if not any(word in extracted_text.lower() for word in ['seized', 'apprehended', 'recovered']):
                return False, "Text mentions drugs but no seizure/apprehension indicated"
        
        # Check if purchase context but no seizure context
        if re.search(SUSPICIOUS_PATTERNS['purchase_only'], extracted_text):
            if 'sold' in extracted_text.lower() or 'supplied' in extracted_text.lower():
                return False, "Text is about supply/sale, not seizure in possession"
        
        return True, ""
    
    def match(self, extracted_name: str, extracted_quantity: Optional[float] = None,
              extracted_unit: Optional[str] = None, extracted_form: Optional[str] = None,
              source_context: Optional[str] = None) -> MatchResult:
        """
        Production-grade match with full edge case handling and audit trail.
        
        Args:
            extracted_name: Drug name from LLM extraction
            extracted_quantity: Quantity seized
            extracted_unit: Unit (kg, g, ml, no, etc.)
            extracted_form: Form (solid, liquid, count)
            source_context: Original brief facts snippet for validation
        
        Returns:
            MatchResult with full audit trail
        """
        audit_log = {
            'step': [],
            'warnings': [],
            'errors': [],
        }
        validation_warnings = []
        
        if not extracted_name or not extracted_name.strip():
            audit_log['errors'].append('Empty extracted_name')
            return MatchResult(
                standard_name=extracted_name or 'Unknown',
                category_group='Unknown',
                raw_kb_name='Unknown',
                match_confidence=0.0,
                match_type='empty_input',
                match_ratio=0.0,
                validation_warnings=['Empty drug name'],
                ai_confidence_boost=-0.50,
                audit_log=audit_log,
            )
        
        # Step 1: Check for false positive (EDGE CASE)
        is_genuine_seizure, fp_reason = self._detect_false_positives(source_context or '', extracted_name)
        if not is_genuine_seizure:
            audit_log['warnings'].append(f'False positive detected: {fp_reason}')
            validation_warnings.append(fp_reason)
            return MatchResult(
                standard_name=extracted_name,
                category_group='Unknown',
                raw_kb_name='Unknown',
                match_confidence=0.0,
                match_type='false_positive_detected',
                match_ratio=0.0,
                validation_warnings=validation_warnings,
                ai_confidence_boost=-0.70,
                audit_log=audit_log,
            )
        
        # Step 2: Check form-unit validity (EDGE CASE)
        if extracted_form and extracted_unit:
            is_valid, reason = self._check_form_unit_validity(extracted_form, extracted_unit)
            if not is_valid:
                audit_log['warnings'].append(f'Form-unit mismatch: {reason}')
                validation_warnings.append(reason)
        
        # Step 3: Check quantity sanity (EDGE CASE)
        if extracted_quantity:
            is_sane, reason = self._check_quantity_sanity(extracted_quantity, extracted_name, extracted_form or '')
            if not is_sane:
                audit_log['warnings'].append(f'Quantity check: {reason}')
                validation_warnings.append(reason)
        
        extracted_norm = self._normalize(extracted_name)
        
        # Step 4: Exact match check
        if extracted_norm in self.kb_exact_index:
            entry = self.kb_exact_index[extracted_norm]
            audit_log['step'].append('exact_indexed_match')
            
            result = MatchResult(
                standard_name=entry['standard'],
                category_group=entry['category'],
                raw_kb_name=entry['raw'],
                match_confidence=1.0,
                match_type='exact_indexed',
                match_ratio=1.0,
                validation_warnings=validation_warnings,
                ai_confidence_boost=self.CONFIDENCE_BOOST_EXACT,
                audit_log=audit_log,
            )
            
            logger.debug(f"Exact match: '{extracted_name}' → '{result.standard_name}'")
            return result
        
        # Step 5: Alias check
        if extracted_norm in self.kb_aliases:
            alias_entry = self.kb_aliases[extracted_norm]
            audit_log['step'].append('tier1_alias_match')
            
            result = MatchResult(
                standard_name=alias_entry['standard'],
                category_group=alias_entry['category'],
                raw_kb_name=alias_entry['standard'],
                match_confidence=0.98,
                match_type='alias_match',
                match_ratio=0.98,
                validation_warnings=validation_warnings,
                ai_confidence_boost=self.CONFIDENCE_BOOST_EXACT,
                audit_log=audit_log,
            )
            
            logger.debug(f"Alias match: '{extracted_name}' → '{result.standard_name}'")
            return result
        
        # Step 6: Fuzzy match
        best_match = None
        best_ratio = 0.0
        best_entry = None
        
        for entry in self.drug_categories:
            raw = str(entry.get('raw_name', '')).strip()
            standard = str(entry.get('standard_name', '')).strip()
            
            if not raw or not standard:
                continue
            
            ratio_raw = self._similarity_ratio(extracted_name, raw)
            ratio_std = self._similarity_ratio(extracted_name, standard)
            ratio = max(ratio_raw, ratio_std)
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_entry = entry
                best_match = standard
        
        audit_log['step'].append(f'fuzzy_match_done (best_ratio={best_ratio:.3f})')
        
        # Step 7: Decision tree based on threshold
        if best_ratio >= self.EXACT_MATCH_THRESHOLD:
            audit_log['step'].append('decision=fuzzy_exact')
            result = MatchResult(
                standard_name=best_match,
                category_group=best_entry['category_group'],
                raw_kb_name=best_entry['raw_name'],
                match_confidence=best_ratio,
                match_type='fuzzy_exact',
                match_ratio=best_ratio,
                validation_warnings=validation_warnings,
                ai_confidence_boost=self.CONFIDENCE_BOOST_HIGH,
                audit_log=audit_log,
            )
            logger.info(f"Fuzzy exact: '{extracted_name}' → '{best_match}' (ratio={best_ratio:.2f})")
            return result
        
        elif best_ratio >= self.HIGH_CONFIDENCE_THRESHOLD:
            audit_log['step'].append('decision=fuzzy_high')
            result = MatchResult(
                standard_name=best_match,
                category_group=best_entry['category_group'],
                raw_kb_name=best_entry['raw_name'],
                match_confidence=best_ratio,
                match_type='fuzzy_high',
                match_ratio=best_ratio,
                validation_warnings=validation_warnings,
                ai_confidence_boost=self.CONFIDENCE_BOOST_HIGH * 0.8,
                audit_log=audit_log,
            )
            logger.info(f"Fuzzy high: '{extracted_name}' → '{best_match}' (ratio={best_ratio:.2f})")
            return result
        
        elif best_ratio >= self.MEDIUM_CONFIDENCE_THRESHOLD:
            audit_log['step'].append('decision=fuzzy_medium_rejected')
            audit_log['warnings'].append(f'Fuzzy medium match rejected (ratio={best_ratio:.2f}), keeping original')
            validation_warnings.append(f"Weak KB match ({best_ratio:.1%}), not refined")
            
            result = MatchResult(
                standard_name=extracted_name,  # Keep original
                category_group=best_entry['category_group'] if best_entry else 'Unknown',
                raw_kb_name=best_entry['raw_name'] if best_entry else 'Unknown',
                match_confidence=best_ratio * 0.6,
                match_type='fuzzy_medium_rejected',
                match_ratio=best_ratio,
                validation_warnings=validation_warnings,
                ai_confidence_boost=-self.CONFIDENCE_DISCOUNT_UNCERTAIN,
                audit_log=audit_log,
            )
            logger.warning(f"Fuzzy medium (rejected): '{extracted_name}' ~{best_ratio:.2f}→{best_match}")
            return result
        
        else:
            audit_log['step'].append('decision=no_match')
            
            result = MatchResult(
                standard_name=extracted_name,
                category_group='Unknown',
                raw_kb_name='Unknown',
                match_confidence=0.3,
                match_type='no_kb_match',
                match_ratio=best_ratio,
                validation_warnings=validation_warnings + ['Not found in KB'],
                ai_confidence_boost=-self.CONFIDENCE_DISCOUNT_SUSPICIOUS,
                audit_log=audit_log,
            )
            logger.debug(f"No KB match: '{extracted_name}' (best ratio={best_ratio:.2f})")
            return result
    
    def _similarity_ratio(self, extracted: str, kb_name: str) -> float:
        """Calculate similarity with substring bonus."""
        norm_extracted = self._normalize(extracted)
        norm_kb = self._normalize(kb_name)
        
        if not norm_extracted or not norm_kb:
            return 0.0
        
        ratio = SequenceMatcher(None, norm_extracted, norm_kb).ratio()
        
        # Bonus for substring matches
        if len(norm_extracted) > 3:
            if norm_extracted in norm_kb or norm_kb in norm_extracted:
                ratio = max(ratio, 0.85)
        
        return ratio


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 3: POST-MATCH VALIDATION & COMMERCIAL QUANTITY CHECK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def validate_commercial_quantity(match_result: MatchResult, 
                                  quantity: float, 
                                  unit: str) -> Tuple[bool, Optional[float]]:
    """
    Validate against NDPS commercial quantity thresholds.
    Returns (is_commercial, threshold_value or None).
    """
    if not quantity or quantity <= 0:
        return False, None
    
    standard_name = match_result.standard_name.lower()
    threshold = COMMERCIAL_QUANTITY_NDPS.get(standard_name)
    
    if not threshold:
        return False, None
    
    # Normalize quantity to standard unit (kg)
    if unit:
        unit_norm = re.sub(r'[^a-z]', '', unit.lower().strip())
        
        if unit_norm in {'gram', 'grams', 'gm', 'gms', 'g', 'gr', 'grm', 'grms'}:
            quantity_kg = quantity / 1000.0
        elif unit_norm in {'kilogram', 'kilograms', 'kg', 'kgs', 'kilo', 'kilos'}:
            quantity_kg = quantity
        elif unit_norm in {'mg', 'milligram', 'milligrams'}:
            quantity_kg = quantity / 1_000_000.0
        else:
            # For count/no/tablets, can't normalize to kg
            return False, None
    else:
        quantity_kg = quantity
    
    is_commercial = quantity_kg >= threshold
    return is_commercial, threshold


def validate_ndps_sections(acts_sections: str) -> Tuple[bool, str]:
    """
    Validate drug seizure against NDPS Act sections mentioned.
    Senior officer would check: "Section 8(c) for possession? Good."
    """
    if not acts_sections:
        return True, ""  # No sections mentioned, can't validate
    
    section_match = re.search(r'(?:section|sec|s\.?|u/s)\s*(\d+(?:\([a-z]\))?)', acts_sections, re.IGNORECASE)
    
    if not section_match:
        return False, "No valid NDPS section found"
    
    section = section_match.group(1)
    
    valid_ndps_sections = ['8', '20', '21', '22', '25', '27', '28', '29']
    
    # Check if section is valid NDPS section
    for valid_section in valid_ndps_sections:
        if section.startswith(valid_section):
            return True, f"Valid NDPS section: {section}"
    
    return False, f"Section {section} not recognized as NDPS Act section"


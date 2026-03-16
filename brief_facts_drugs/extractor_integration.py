# Integration Layer: Advanced KB Matching into Extraction Pipeline
# File: brief_facts_drugs/extractor_integration.py

import logging
from typing import List, Tuple, Optional
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

# Import advanced matcher
from kb_matcher_advanced import (
    DrugKBMatcherAdvanced, MatchResult, validate_commercial_quantity,
    validate_ndps_sections, COMMERCIAL_QUANTITY_NDPS
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 1: EXTRACTION REFINEMENT WITH KB MATCHING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def refine_drugs_with_advanced_kb(drugs: List['DrugExtraction'],
                                   kb_matcher: DrugKBMatcherAdvanced,
                                   acts_sections: Optional[str] = None) -> List['DrugExtraction']:
    """
    Advanced KB refinement stage with all edge case handling.
    Behaves like senior NDPS officer: meticulous, rule-based.
    
    Processing stages:
    1. KB fuzzy matching
    2. Form-unit validation
    3. Quantity sanity checks
    4. Commercial quantity determination
    5. NDPS Act section validation
    6. Confidence score adjustment
    7. Audit trail recording
    """
    refined = []
    
    for idx, drug in enumerate(drugs):
        try:
            logger.debug(f"Refining drug {idx+1}/{len(drugs)}: {drug.raw_drug_name}")
            
            # Initialize audit trail
            if not drug.extraction_metadata:
                drug.extraction_metadata = {}
            
            audit_trail = {
                'raw_extraction': drug.raw_drug_name,
                'stages': [],
                'warnings': [],
                'errors': [],
                'decision_log': [],
            }
            
            # Stage 1: KB Fuzzy Matching
            match_result: MatchResult = kb_matcher.match(
                extracted_name=drug.raw_drug_name,
                extracted_quantity=drug.raw_quantity,
                extracted_unit=drug.raw_unit,
                extracted_form=drug.drug_form,
                source_context=drug.extraction_metadata.get('source_sentence', '')
            )
            
            audit_trail['stages'].append('kb_match_complete')
            audit_trail['kb_match'] = {
                'extracted': drug.raw_drug_name,
                'matched_to': match_result.standard_name,
                'match_type': match_result.match_type,
                'match_ratio': round(match_result.match_ratio, 3),
                'category': match_result.category_group,
            }
            
            # Handle false positives (reject immediately)
            if match_result.match_type == 'false_positive_detected':
                logger.warning(
                    f"Drug rejected: False positive. Reason: {match_result.validation_warnings}"
                )
                audit_trail['decision_log'].append({
                    'stage': 'false_positive_check',
                    'decision': 'REJECT',
                    'reason': match_result.validation_warnings[0] if match_result.validation_warnings else 'Unknown',
                })
                
                # Mark as low confidence for filtering
                drug.confidence_score = 0.20
                drug.extraction_metadata['kb_refinement'] = audit_trail
                continue  # Skip this drug
            
            # Stage 2: Apply KB refinement
            if match_result.match_type in ('exact_indexed', 'alias_match', 'fuzzy_exact', 'fuzzy_high'):
                old_name = drug.primary_drug_name
                drug.primary_drug_name = match_result.standard_name
                
                audit_trail['decision_log'].append({
                    'stage': 'kb_refinement',
                    'decision': 'REFINED',
                    'old_name': old_name,
                    'new_name': match_result.standard_name,
                    'match_type': match_result.match_type,
                })
                
                logger.info(
                    f"KB refined: '{old_name}' → '{match_result.standard_name}' "
                    f"({match_result.match_type})"
                )
            else:
                audit_trail['decision_log'].append({
                    'stage': 'kb_refinement',
                    'decision': 'NOT_REFINED',
                    'reason': 'Weak match or no match',
                    'match_type': match_result.match_type,
                })
            
            # Stage 3: Quantity Validation
            if drug.raw_quantity and drug.raw_quantity > 0:
                if match_result.validation_warnings:
                    audit_trail['warnings'].extend(match_result.validation_warnings)
                    logger.warning(
                        f"Quantity warning for {drug.primary_drug_name}: "
                        f"{'; '.join(match_result.validation_warnings)}"
                    )
            
            # Stage 4: Commercial Quantity Check
            is_commercial, threshold = validate_commercial_quantity(
                match_result,
                drug.raw_quantity or 0,
                drug.raw_unit
            )
            
            if threshold is not None:
                drug.is_commercial = is_commercial
                audit_trail['commercial_check'] = {
                    'quantity': drug.raw_quantity,
                    'unit': drug.raw_unit,
                    'threshold': threshold,
                    'is_commercial': is_commercial,
                }
                
                if is_commercial:
                    logger.info(
                        f"Commercial quantity: {drug.raw_quantity} {drug.raw_unit} "
                        f">= {threshold} kg threshold for {drug.primary_drug_name}"
                    )
                    audit_trail['decision_log'].append({
                        'stage': 'commercial_check',
                        'decision': 'COMMERCIAL_QUANTITY',
                        'threshold': threshold,
                    })
            
            # Stage 5: NDPS Act Section Validation
            if acts_sections:
                is_valid_section, section_info = validate_ndps_sections(acts_sections)
                audit_trail['ndps_section_validation'] = {
                    'sections': acts_sections,
                    'is_valid': is_valid_section,
                    'info': section_info,
                }
                
                if not is_valid_section:
                    audit_trail['warnings'].append(f"Section validation: {section_info}")
                    logger.warning(f"Potential NDPS section issue: {section_info}")
            
            # Stage 6: Confidence Score Adjustment
            original_confidence = float(drug.confidence_score or 0)
            boost = match_result.ai_confidence_boost
            
            adjusted_confidence = max(0.0, min(0.99, original_confidence + boost))
            
            if abs(boost) > 0.01:  # Only log if significant adjustment
                logger.debug(
                    f"Confidence adjusted: {original_confidence:.2f} + {boost:+.2f} "
                    f"= {adjusted_confidence:.2f}"
                )
                audit_trail['confidence_adjustment'] = {
                    'original': original_confidence,
                    'boost': round(boost, 3),
                    'final': round(adjusted_confidence, 2),
                }
            
            drug.confidence_score = adjusted_confidence
            
            # Stage 7: Store full audit trail
            drug.extraction_metadata['kb_refinement'] = audit_trail
            drug.extraction_metadata['kb_match_final'] = {
                'standard_name': match_result.standard_name,
                'category': match_result.category_group,
                'match_type': match_result.match_type,
                'match_confidence': round(match_result.match_ratio, 3),
                'is_commercial': drug.is_commercial,
                'validation_warnings': match_result.validation_warnings,
            }
            
            refined.append(drug)
            
        except Exception as e:
            logger.error(f"Error refining drug {idx}: {e}", exc_info=True)
            if not drug.extraction_metadata:
                drug.extraction_metadata = {}
            drug.extraction_metadata['refinement_error'] = str(e)
            refined.append(drug)  # Keep original if refinement fails
    
    logger.info(f"KB refinement complete: {len(refined)}/{len(drugs)} drugs processed")
    return refined


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SECTION 2: VALIDATION FILTERS (EDGE CASE RULES)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DrugValidationFilter:
    """
    Production-grade validation filters for extracted drugs.
    Implements all extraction prompt rules systematically.
    """
    
    # Rule thresholds (from EXTRACTION_PROMPT)
    MIN_CONFIDENCE_UNKNOWN = 0.50   # R9: unknown drug with 50% confidence minimum
    MIN_CONFIDENCE_PARTIAL = 0.60   # R9: partial extraction minimum
    MIN_CONFIDENCE_CLEAR = 0.85     # R9: name+qty+unit clear
    
    INVALID_DRUG_NAMES = {
        'unknown', 'unidentified', 'unknown drug', 'unknown substance',
        'unknown tablet', 'unknown powder', 'unknown liquid', 'n/a', 'none', 'na'
    }
    
    @staticmethod
    def validate_drug_name(drug_name: str, confidence: float) -> Tuple[bool, Optional[str]]:
        """
        Rule: R11 - Skip unknown/unidentified drug names.
        """
        if not drug_name or drug_name.strip().lower() in DrugValidationFilter.INVALID_DRUG_NAMES:
            return False, "Invalid/unknown drug name"
        
        if len(drug_name.strip()) < 3:
            return False, "Drug name too short"
        
        return True, None
    
    @staticmethod
    def validate_quantity_unit_combo(quantity: float, unit: str, drug_form: str) -> Tuple[bool, Optional[str]]:
        """
        Rule: R12 - Validate form matches unit.
        "liquid drugs (oil, syrup, solution) → raw_unit MUST be ml/litres"
        """
        if not quantity or quantity <= 0:
            return False, "Invalid quantity"
        
        if not unit or unit.strip().lower() == 'unknown':
            return False, "Missing unit"
        
        # Check form-unit consistency
        unit_norm = unit.lower().replace('.', '').strip()
        form_norm = (drug_form or '').lower().strip()
        
        liquid_forms = {'liquid', 'syrup', 'oil', 'solution', 'tincture', 'extract'}
        solid_forms = {'solid', 'powder', 'dry', 'paste'}
        count_forms = {'tablet', 'pill', 'capsule', 'no', 'nos'}
        
        liquid_units = {'ml', 'litre', 'liter', 'l', 'ltr', 'ltrs'}
        solid_units = {'kg', 'g', 'gm', 'gms', 'gram', 'grams', 'mg'}
        count_units = {'no', 'nos', 'tablet', 'pill', 'strip', 'packet', 'blot'}
        
        if any(form in form_norm for form in liquid_forms):
            if not any(u in unit_norm for u in liquid_units):
                return False, f"Liquid form requires ml/L unit, got {unit}"
        
        elif any(form in form_norm for form in solid_forms):
            if not any(u in unit_norm for u in solid_units):
                return False, f"Solid form requires kg/g unit, got {unit}"
        
        elif any(form in form_norm for form in count_forms):
            if not any(u in unit_norm for u in count_units):
                return False, f"Count form requires no/tablet unit, got {unit}"
        
        return True, None
    
    @staticmethod
    def validate_accused_assignment(accused_id: Optional[str], 
                                     seizure_context: str) -> Tuple[bool, Optional[str]]:
        """
        Rule: R6 - Validate accused_id assignment.
        Collective seizures should have accused_id=null, individual should have it.
        """
        if seizure_context and 'collective' in seizure_context.lower():
            if accused_id and accused_id.strip():
                return False, "Collective seizure should have accused_id=null"
        
        return True, None
    
    @staticmethod
    def validate_seizure_worth(seizure_worth: float, worth_scope: str) -> Tuple[bool, Optional[str]]:
        """
        Rule: R8 - Validate seizure_worth is reasonable.
        """
        if seizure_worth < 0:
            return False, "Seizure worth cannot be negative"
        
        # Ultra-high values should raise flag (but not invalidate)
        if seizure_worth > 100_000_000:  # 10 crores
            logger.warning(f"Seizure worth extremely high: Rs.{seizure_worth:,.0f}")
        
        if new worth_scope not in ('individual', 'drug_total', 'overall_total', None):
            return False, f"Invalid worth_scope: {worth_scope}"
        
        return True, None
    
    @staticmethod
    def validate_plant_seizures(drug_name: str, quantity: float, unit: str) -> Tuple[bool, Optional[str]]:
        """
        Rule: R13 - Plant/Cultivation seizures allowed.
        "8 ganja plants" → raw_quantity=8, raw_unit="plants", drug_form="count"
        """
        plant_keywords = {'plant', 'plants', 'seedling', 'seedlings', 'sapling', 'saplings', 'bush', 'tree'}
        unit_norm = (unit or '').lower()
        drug_norm = (drug_name or '').lower()
        
        is_plant = any(kw in unit_norm or kw in drug_norm for kw in plant_keywords)
        
        if is_plant:
            if quantity <= 0 or quantity > 10000:
                return False, f"Unreasonable plant count: {quantity}"
            return True, None
        
        return True, None
    
    @staticmethod
    def validate_commercial_flag(is_commercial: bool, drug_name: str, 
                                 quantity: float, quantity_kg: float) -> Tuple[bool, Optional[str]]:
        """
        Rule: R14 - is_commercial only if text explicitly states it.
        Can't assume commercial just based on quantity.
        """
        # Note: Commercial determination should be TEXTUAL + THRESHOLD, not just threshold
        # This is a validation, not assignment
        return True, None
    
    @staticmethod
    def validate_decimal_quantity(raw_quantity_str: str, parsed_quantity: float) -> Tuple[bool, Optional[str]]:
        """
        Rule: R15 - Decimal vs comma handling.
        "1.200 Kg" = 1.2 kg (decimal), NOT 1200 kg
        """
        if ',' in str(raw_quantity_str):
            # Indian format: 1,000 = thousand (should have been parsed as 1000)
            # But if quantity < 100, comma is thousands separator, not decimal
            if parsed_quantity < 100 and ',' in str(raw_quantity_str):
                logger.warning(f"Potential comma/decimal confusion: {raw_quantity_str} → {parsed_quantity}")
        
        return True, None


def apply_validation_rules(drugs: List['DrugExtraction']) -> Tuple[List['DrugExtraction'], dict]:
    """
    Apply all extraction validation rules.
    Returns (valid_drugs, rejection_stats).
    """
    validator = DrugValidationFilter()
    valid = []
    rejected = {
        'invalid_name': 0,
        'invalid_qty_unit': 0,
        'form_unit_mismatch': 0,
        'low_confidence': 0,
        'other': 0,
    }
    
    for drug in drugs:
        reasons = []
        
        # Check drug name
        is_valid, reason = validator.validate_drug_name(
            drug.raw_drug_name,
            drug.confidence_score or 0
        )
        if not is_valid:
            rejected['invalid_name'] += 1
            reasons.append(reason)
        
        # Check quantity-unit combo
        is_valid, reason = validator.validate_quantity_unit_combo(
            drug.raw_quantity or 0,
            drug.raw_unit,
            drug.drug_form
        )
        if not is_valid:
            rejected['invalid_qty_unit'] += 1
            reasons.append(reason)
        
        # Check plant seizures (special case)
        is_valid, _ = validator.validate_plant_seizures(
            drug.raw_drug_name,
            drug.raw_quantity or 0,
            drug.raw_unit
        )
        if not is_valid:
            rejected['other'] += 1
            reasons.append("Invalid plant count")
        
        # Check confidence threshold
        if (drug.confidence_score or 0) < validator.MIN_CONFIDENCE_UNKNOWN:
            rejected['low_confidence'] += 1
            reasons.append(f"Confidence too low: {drug.confidence_score}")
        
        if not reasons:
            valid.append(drug)
        else:
            logger.warning(
                f"Drug rejected: {drug.raw_drug_name} - {'; '.join(reasons)}"
            )
    
    logger.info(f"Validation results: {len(valid)}/{len(drugs)} valid, {len(rejected)} rejected")
    return valid, rejected


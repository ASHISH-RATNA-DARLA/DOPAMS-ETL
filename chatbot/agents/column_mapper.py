"""
Intelligent Column Mapper
Maps user question keywords to actual database columns automatically
This ensures the system KNOWS which columns to use without guessing!
"""

import re
import logging
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ColumnMatch:
    """Represents a column match from user question"""
    table: str
    column: str
    alias: Optional[str] = None
    confidence: float = 1.0
    keywords_matched: List[str] = None
    
    def __post_init__(self):
        if self.keywords_matched is None:
            self.keywords_matched = []


class IntelligentColumnMapper:
    """
    Maps user question keywords to actual database columns
    
    This is the KEY intelligence: Instead of LLM guessing,
    we KNOW which columns exist and map user questions to them!
    """
    
    def __init__(self):
        """Initialize with comprehensive column mappings"""
        # Build keyword-to-column mapping from schema knowledge
        self.column_mappings = self._build_column_mappings()
        logger.info(f"Initialized column mapper with {len(self.column_mappings)} mappings")
    
    def _build_column_mappings(self) -> Dict[str, List[ColumnMatch]]:
        """
        Build comprehensive keyword-to-column mappings
        
        Format: keyword -> [ColumnMatch(table, column, alias, confidence)]
        """
        mappings = {}
        
        # ====================================================================
        # DRUG-RELATED COLUMNS (brief_facts_drugs table)
        # ====================================================================
        drug_mappings = {
            # Transport & Logistics
            'transport': [ColumnMatch('brief_facts_drugs', 'transport_method', 'transport_method', 1.0, ['transport'])],
            'transport method': [ColumnMatch('brief_facts_drugs', 'transport_method', 'transport_method', 1.0, ['transport', 'method'])],
            'method of transport': [ColumnMatch('brief_facts_drugs', 'transport_method', 'transport_method', 1.0, ['method', 'transport'])],
            'how transported': [ColumnMatch('brief_facts_drugs', 'transport_method', 'transport_method', 1.0, ['transported'])],
            
            # Packaging
            'packaging': [ColumnMatch('brief_facts_drugs', 'packaging_details', 'packaging_details', 1.0, ['packaging'])],
            'packaging details': [ColumnMatch('brief_facts_drugs', 'packaging_details', 'packaging_details', 1.0, ['packaging', 'details'])],
            'package': [ColumnMatch('brief_facts_drugs', 'packaging_details', 'packaging_details', 0.9, ['package'])],
            'packed': [ColumnMatch('brief_facts_drugs', 'packaging_details', 'packaging_details', 0.8, ['packed'])],
            'number of packets': [ColumnMatch('brief_facts_drugs', 'number_of_packets', 'number_of_packets', 1.0, ['packets'])],
            'packets': [ColumnMatch('brief_facts_drugs', 'number_of_packets', 'number_of_packets', 0.9, ['packets'])],
            
            # Supply Chain
            'supply chain': [ColumnMatch('brief_facts_drugs', 'supply_chain', 'supply_chain', 1.0, ['supply', 'chain'])],
            'supply': [ColumnMatch('brief_facts_drugs', 'supply_chain', 'supply_chain', 0.8, ['supply'])],
            'source location': [ColumnMatch('brief_facts_drugs', 'source_location', 'source_location', 1.0, ['source'])],
            'source': [ColumnMatch('brief_facts_drugs', 'source_location', 'source_location', 0.9, ['source'])],
            'destination': [ColumnMatch('brief_facts_drugs', 'destination', 'destination', 1.0, ['destination'])],
            'where from': [ColumnMatch('brief_facts_drugs', 'source_location', 'source_location', 0.8, ['from'])],
            'where to': [ColumnMatch('brief_facts_drugs', 'destination', 'destination', 0.8, ['to'])],
            
            # Weight & Quantity
            'weight': [ColumnMatch('brief_facts_drugs', 'weight_breakdown', 'weight_breakdown', 0.9, ['weight']),
                      ColumnMatch('brief_facts_drugs', 'total_quantity', 'total_quantity', 0.8, ['weight'])],
            'weight breakdown': [ColumnMatch('brief_facts_drugs', 'weight_breakdown', 'weight_breakdown', 1.0, ['weight', 'breakdown'])],
            'quantity': [ColumnMatch('brief_facts_drugs', 'total_quantity', 'total_quantity', 0.9, ['quantity']),
                        ColumnMatch('brief_facts_drugs', 'quantity_numeric', 'quantity_numeric', 0.8, ['quantity'])],
            'total quantity': [ColumnMatch('brief_facts_drugs', 'total_quantity', 'total_quantity', 1.0, ['total', 'quantity'])],
            'quantity unit': [ColumnMatch('brief_facts_drugs', 'quantity_unit', 'quantity_unit', 1.0, ['unit'])],
            
            # Seizure
            'seizure location': [ColumnMatch('brief_facts_drugs', 'seizure_location', 'seizure_location', 1.0, ['seizure', 'location'])],
            'seizure time': [ColumnMatch('brief_facts_drugs', 'seizure_time', 'seizure_time', 1.0, ['seizure', 'time'])],
            'seizure method': [ColumnMatch('brief_facts_drugs', 'seizure_method', 'seizure_method', 1.0, ['seizure', 'method'])],
            'seizure officer': [ColumnMatch('brief_facts_drugs', 'seizure_officer', 'seizure_officer', 1.0, ['seizure', 'officer'])],
            'seizure worth': [ColumnMatch('brief_facts_drugs', 'seizure_worth', 'seizure_worth', 1.0, ['seizure', 'worth'])],
            'seizure value': [ColumnMatch('brief_facts_drugs', 'seizure_worth', 'seizure_worth', 1.0, ['seizure', 'value'])],
            'drugs with seizure worth': [ColumnMatch('brief_facts_drugs', 'seizure_worth', 'seizure_worth', 1.0, ['seizure', 'worth'])],
            # Note: 'seized' is now in property_mappings with higher priority when "property" is mentioned
            # Only match drug seizure if "drug" or "narcotic" is also mentioned
            'drug seized': [ColumnMatch('brief_facts_drugs', 'seizure_location', 'seizure_location', 1.0, ['drug', 'seized'])],
            'seized drug': [ColumnMatch('brief_facts_drugs', 'seizure_location', 'seizure_location', 1.0, ['seized', 'drug'])],
            # Source location (for "seized from" queries)
            'seized from': [ColumnMatch('brief_facts_drugs', 'source_location', 'source_location', 1.0, ['seized', 'from'])],
            'seized at': [ColumnMatch('brief_facts_drugs', 'seizure_location', 'seizure_location', 1.0, ['seized', 'at'])],
            
            # Commercial Quantity
            'commercial quantity': [ColumnMatch('brief_facts_drugs', 'is_commercial', 'is_commercial', 1.0, ['commercial', 'quantity']),
                                   ColumnMatch('brief_facts_drugs', 'commercial_quantity', 'commercial_quantity', 0.9, ['commercial', 'quantity'])],
            'commercial': [ColumnMatch('brief_facts_drugs', 'is_commercial', 'is_commercial', 0.9, ['commercial'])],
            'is commercial': [ColumnMatch('brief_facts_drugs', 'is_commercial', 'is_commercial', 1.0, ['commercial'])],
            
            # Value
            'street value': [ColumnMatch('brief_facts_drugs', 'street_value', 'street_value', 1.0, ['street', 'value']),
                            ColumnMatch('brief_facts_drugs', 'street_value_numeric', 'street_value_numeric', 0.9, ['street', 'value'])],
            'value': [ColumnMatch('brief_facts_drugs', 'street_value', 'street_value', 0.7, ['value']),
                     ColumnMatch('brief_facts_drugs', 'seizure_worth', 'seizure_worth', 0.7, ['value'])],
            'worth': [ColumnMatch('brief_facts_drugs', 'seizure_worth', 'seizure_worth', 0.9, ['worth'])],
            
            # Purity
            'purity': [ColumnMatch('brief_facts_drugs', 'purity', 'purity', 1.0, ['purity'])],
            
            # Drug Info
            'drug name': [ColumnMatch('brief_facts_drugs', 'drug_name', 'drug_name', 1.0, ['drug', 'name'])],
            'scientific name': [ColumnMatch('brief_facts_drugs', 'scientific_name', 'scientific_name', 1.0, ['scientific'])],
            'brand name': [ColumnMatch('brief_facts_drugs', 'brand_name', 'brand_name', 1.0, ['brand'])],
            'drug category': [ColumnMatch('brief_facts_drugs', 'drug_category', 'drug_category', 1.0, ['category'])],
            'drug schedule': [ColumnMatch('brief_facts_drugs', 'drug_schedule', 'drug_schedule', 1.0, ['schedule'])],
        }
        
        # ====================================================================
        # PERSON-RELATED COLUMNS
        # ====================================================================
        person_mappings = {
            # Phone
            'phone': [ColumnMatch('persons', 'phone_number', 'phone_number', 1.0, ['phone']),
                     ColumnMatch('brief_facts_accused', 'phone_numbers', 'phone_numbers', 0.9, ['phone'])],
            'phone number': [ColumnMatch('persons', 'phone_number', 'phone_number', 1.0, ['phone', 'number']),
                            ColumnMatch('brief_facts_accused', 'phone_numbers', 'phone_numbers', 0.9, ['phone', 'number'])],
            'mobile': [ColumnMatch('persons', 'phone_number', 'phone_number', 1.0, ['mobile']),
                      ColumnMatch('brief_facts_accused', 'phone_numbers', 'phone_numbers', 0.9, ['mobile'])],
            'mobile number': [ColumnMatch('persons', 'phone_number', 'phone_number', 1.0, ['mobile', 'number']),
                             ColumnMatch('brief_facts_accused', 'phone_numbers', 'phone_numbers', 0.9, ['mobile', 'number'])],
            'contact': [ColumnMatch('persons', 'phone_number', 'phone_number', 0.8, ['contact'])],
            
            # Email
            'email': [ColumnMatch('persons', 'email_id', 'email_id', 1.0, ['email'])],
            'email address': [ColumnMatch('persons', 'email_id', 'email_id', 1.0, ['email', 'address'])],
            'email id': [ColumnMatch('persons', 'email_id', 'email_id', 1.0, ['email', 'id'])],
            
            # Nationality
            'nationality': [ColumnMatch('persons', 'nationality', 'nationality', 1.0, ['nationality'])],
            
            # District (present_district in actual schema)
            'district': [ColumnMatch('persons', 'present_district', 'present_district', 1.0, ['district']),
                        ColumnMatch('hierarchy', 'dist_name', 'dist_name', 0.9, ['district'])],
            'from district': [ColumnMatch('persons', 'present_district', 'present_district', 1.0, ['district'])],
            'present district': [ColumnMatch('persons', 'present_district', 'present_district', 1.0, ['present', 'district'])],
            
            # State (present_state_ut in actual schema)
            'state': [ColumnMatch('persons', 'present_state_ut', 'present_state_ut', 1.0, ['state']),
                     ColumnMatch('hierarchy', 'zone_name', 'zone_name', 0.8, ['state'])],
            'from state': [ColumnMatch('persons', 'present_state_ut', 'present_state_ut', 1.0, ['state'])],
            'present state': [ColumnMatch('persons', 'present_state_ut', 'present_state_ut', 1.0, ['present', 'state'])],
            'telangana': [ColumnMatch('persons', 'present_state_ut', 'present_state_ut', 1.0, ['telangana'])],
            
            # Education
            'education': [ColumnMatch('persons', 'education_qualification', 'education_qualification', 1.0, ['education'])],
            'education qualification': [ColumnMatch('persons', 'education_qualification', 'education_qualification', 1.0, ['education', 'qualification'])],
            'qualification': [ColumnMatch('persons', 'education_qualification', 'education_qualification', 0.9, ['qualification'])],
            
            # Physical Features (from accused table)
            'height': [ColumnMatch('accused', 'height', 'height', 1.0, ['height'])],
            'build': [ColumnMatch('accused', 'build', 'build', 1.0, ['build']),
                     ColumnMatch('accused', 'build', 'build_type', 0.9, ['build', 'type'])],
            'build type': [ColumnMatch('accused', 'build', 'build', 1.0, ['build', 'type'])],
            'color': [ColumnMatch('accused', 'color', 'color', 1.0, ['color']),
                     ColumnMatch('accused', 'color', 'complexion', 0.9, ['color'])],
            'complexion': [ColumnMatch('accused', 'color', 'color', 1.0, ['complexion'])],
            'mole': [ColumnMatch('accused', 'mole', 'mole', 1.0, ['mole'])],
            'leucoderma': [ColumnMatch('accused', 'leucoderma', 'leucoderma', 1.0, ['leucoderma'])],
            'physical features': [ColumnMatch('accused', 'height', 'height', 0.8, ['physical', 'features']),
                                ColumnMatch('accused', 'build', 'build', 0.8, ['physical', 'features']),
                                ColumnMatch('accused', 'color', 'color', 0.8, ['physical', 'features'])],
            
            # Name
            'name': [ColumnMatch('persons', 'full_name', 'full_name', 0.9, ['name']),
                    ColumnMatch('persons', 'name', 'name', 0.8, ['name'])],
            'full name': [ColumnMatch('persons', 'full_name', 'full_name', 1.0, ['full', 'name'])],
            'person name': [ColumnMatch('persons', 'full_name', 'full_name', 1.0, ['person', 'name'])],
        }
        
        # ====================================================================
        # PROPERTY-RELATED COLUMNS
        # ====================================================================
        property_mappings = {
            # Property status (HIGH PRIORITY when "property" or "properties" is mentioned)
            'seized properties': [ColumnMatch('properties', 'property_status', 'property_status', 1.0, ['seized', 'properties'])],
            'properties by status': [ColumnMatch('properties', 'property_status', 'property_status', 1.0, ['properties', 'status'])],
            'property status': [ColumnMatch('properties', 'property_status', 'property_status', 1.0, ['property', 'status'])],
            'seized property': [ColumnMatch('properties', 'property_status', 'property_status', 1.0, ['seized', 'property'])],
            'seized': [ColumnMatch('properties', 'property_status', 'property_status', 0.9, ['seized'])],  # Higher priority than drug seizure_location
            
            # Property category
            'properties by category': [ColumnMatch('properties', 'category', 'property_category', 1.0, ['properties', 'category'])],
            'property category': [ColumnMatch('properties', 'category', 'property_category', 1.0, ['property', 'category'])],
            'by category': [ColumnMatch('properties', 'category', 'property_category', 0.9, ['category'])],
            'category': [ColumnMatch('properties', 'category', 'property_category', 0.8, ['category'])],
            
            # Property nature
            'properties by nature': [ColumnMatch('properties', 'nature', 'property_nature', 1.0, ['properties', 'nature'])],
            'property nature': [ColumnMatch('properties', 'nature', 'property_nature', 1.0, ['property', 'nature'])],
            'by nature': [ColumnMatch('properties', 'nature', 'property_nature', 0.9, ['nature'])],
            'nature': [ColumnMatch('properties', 'nature', 'property_nature', 0.8, ['nature'])],
            
            # Other property fields
            'property details': [ColumnMatch('properties', 'particular_of_property', 'particular_of_property', 1.0, ['property', 'details'])],
            'property value': [ColumnMatch('properties', 'estimate_value', 'estimate_value', 0.9, ['property', 'value']),
                              ColumnMatch('properties', 'recovered_value', 'recovered_value', 0.9, ['property', 'value'])],
            'recovered value': [ColumnMatch('properties', 'recovered_value', 'recovered_value', 1.0, ['recovered', 'value'])],
            'estimate value': [ColumnMatch('properties', 'estimate_value', 'estimate_value', 1.0, ['estimate', 'value'])],
            'recovered from': [ColumnMatch('properties', 'recovered_from', 'recovered_from', 1.0, ['recovered', 'from'])],
            'place of recovery': [ColumnMatch('properties', 'place_of_recovery', 'place_of_recovery', 1.0, ['recovery'])],
            'date of seizure': [ColumnMatch('properties', 'date_of_seizure', 'date_of_seizure', 1.0, ['seizure', 'date'])],
        }
        
        # ====================================================================
        # CRIME-RELATED COLUMNS
        # ====================================================================
        crime_mappings = {
            'fir number': [ColumnMatch('crimes', 'fir_num', 'fir_num', 1.0, ['fir', 'number'])],
            'fir': [ColumnMatch('crimes', 'fir_num', 'fir_num', 0.9, ['fir'])],
            'case status': [ColumnMatch('crimes', 'case_status', 'case_status', 1.0, ['case', 'status'])],
            'status': [ColumnMatch('crimes', 'case_status', 'case_status', 0.8, ['status'])],
            'pending cases': [ColumnMatch('crimes', 'case_status', 'case_status', 1.0, ['pending', 'cases'])],
            'pending': [ColumnMatch('crimes', 'case_status', 'case_status', 0.9, ['pending'])],
            'acts sections': [ColumnMatch('crimes', 'acts_sections', 'acts_sections', 1.0, ['acts', 'sections'])],
            'sections': [ColumnMatch('crimes', 'acts_sections', 'acts_sections', 0.9, ['sections'])],
            'crime type': [ColumnMatch('crimes', 'crime_type', 'crime_type', 1.0, ['crime', 'type'])],
            'classification': [ColumnMatch('crimes', 'class_classification', 'class_classification', 1.0, ['classification'])],
            'intermediate': [ColumnMatch('crimes', 'class_classification', 'class_classification', 1.0, ['intermediate'])],
            'io rank': [ColumnMatch('crimes', 'io_rank', 'io_rank', 1.0, ['io', 'rank'])],
            'io name': [ColumnMatch('crimes', 'io_name', 'io_name', 1.0, ['io', 'name'])],
            'investigating officer': [ColumnMatch('crimes', 'io_name', 'io_name', 0.9, ['investigating', 'officer']),
                                    ColumnMatch('crimes', 'io_rank', 'io_rank', 0.9, ['investigating', 'officer'])],
            'inspector': [ColumnMatch('crimes', 'io_rank', 'io_rank', 1.0, ['inspector'])],
        }
        
        # Merge all mappings
        all_mappings = {}
        for mapping_dict in [drug_mappings, person_mappings, property_mappings, crime_mappings]:
            all_mappings.update(mapping_dict)
        
        return all_mappings
    
    def find_columns(self, user_question: str) -> List[ColumnMatch]:
        """
        Find all matching columns for a user question
        
        Args:
            user_question: User's question/query
            
        Returns:
            List of ColumnMatch objects sorted by confidence
        """
        question_lower = user_question.lower()
        matches = []
        matched_keywords = set()
        
        # ⭐ CONTEXT-AWARE: Detect if this is a property-related query
        is_property_query = any(kw in question_lower for kw in ['property', 'properties', 'seized property', 'seized properties'])
        is_drug_query = any(kw in question_lower for kw in ['drug', 'narcotic', 'ganja', 'heroin', 'cocaine', 'substance'])
        
        # Check each keyword mapping
        for keyword, column_matches in self.column_mappings.items():
            # Check if keyword appears in question
            if keyword in question_lower:
                for col_match in column_matches:
                    # ⭐ PRIORITIZE: If property query, boost property table matches
                    # If drug query, boost drug table matches
                    adjusted_confidence = col_match.confidence
                    if is_property_query and col_match.table == 'properties':
                        adjusted_confidence += 0.2  # Boost property matches
                    elif is_drug_query and col_match.table == 'brief_facts_drugs':
                        adjusted_confidence += 0.2  # Boost drug matches
                    elif is_property_query and col_match.table == 'brief_facts_drugs' and 'seized' in keyword:
                        adjusted_confidence -= 0.3  # Reduce drug seizure_location when property query
                    
                    # Avoid duplicates
                    match_key = (col_match.table, col_match.column)
                    if match_key not in matched_keywords:
                        # Create new match with adjusted confidence
                        adjusted_match = ColumnMatch(
                            col_match.table,
                            col_match.column,
                            col_match.alias,
                            adjusted_confidence,
                            col_match.keywords_matched
                        )
                        matches.append(adjusted_match)
                        matched_keywords.add(match_key)
        
        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x.confidence, reverse=True)
        
        if matches:
            logger.info(f"Found {len(matches)} column matches for question: {user_question[:50]}...")
            for match in matches[:5]:  # Log top 5
                logger.debug(f"  → {match.table}.{match.column} (confidence: {match.confidence})")
        
        return matches
    
    def get_required_tables(self, user_question: str) -> Set[str]:
        """
        Get set of tables that need to be joined based on user question
        
        Args:
            user_question: User's question/query
            
        Returns:
            Set of table names
        """
        matches = self.find_columns(user_question)
        tables = {match.table for match in matches}
        return tables
    
    def get_required_columns(self, user_question: str, table: Optional[str] = None) -> List[ColumnMatch]:
        """
        Get columns required for a user question, optionally filtered by table
        
        Args:
            user_question: User's question/query
            table: Optional table name to filter by
            
        Returns:
            List of ColumnMatch objects
        """
        matches = self.find_columns(user_question)
        if table:
            matches = [m for m in matches if m.table == table]
        return matches
    
    def build_column_list(self, user_question: str, table_prefix: str = '') -> List[str]:
        """
        Build a list of column names with table prefixes for SQL queries
        
        Args:
            user_question: User's question/query
            table_prefix: Optional prefix (e.g., 'd.' for brief_facts_drugs)
            
        Returns:
            List of column names with prefixes (e.g., ['d.transport_method', 'd.packaging_details'])
        """
        matches = self.find_columns(user_question)
        
        # Group by table to determine prefix
        columns = []
        for match in matches:
            if table_prefix:
                col_name = f"{table_prefix}.{match.column}"
            else:
                col_name = f"{match.table}.{match.column}"
            
            # Add alias if specified
            if match.alias and match.alias != match.column:
                col_name = f"{col_name} as {match.alias}"
            
            columns.append(col_name)
        
        return columns



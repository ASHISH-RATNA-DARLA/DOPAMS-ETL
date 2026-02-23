"""
Smart Schema Manager - Professional Version
Intelligently selects only relevant schema based on user query
"""

import re
import logging
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, field
from functools import lru_cache
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class ColumnPriority(Enum):
    """Column importance levels"""
    CRITICAL = 1  # id, primary keys
    HIGH = 2      # name, email, date fields
    MEDIUM = 3    # status, type, code
    LOW = 4       # other fields

@dataclass
class ColumnInfo:
    """Enhanced column information"""
    name: str
    data_type: str
    nullable: bool
    priority: ColumnPriority = ColumnPriority.LOW
    is_key: bool = False
    
    @classmethod
    def from_dict(cls, col_dict: Dict) -> 'ColumnInfo':
        """Create from dictionary"""
        return cls(
            name=col_dict['column'],
            data_type=col_dict['type'],
            nullable=col_dict.get('nullable', True)
        )

@dataclass
class TableInfo:
    """Table metadata"""
    name: str
    columns: List[ColumnInfo]
    row_count: Optional[int] = None
    relevance_score: float = 0.0
    
    def get_key_columns(self, max_count: int = 10) -> List[ColumnInfo]:
        """Get most important columns sorted by priority"""
        sorted_cols = sorted(self.columns, key=lambda c: c.priority.value)
        return sorted_cols[:max_count]

@dataclass
class SchemaConfig:
    """Configuration for schema selection"""
    max_tables: int = 3
    max_columns: int = 10
    show_data_types: bool = True
    show_nullability: bool = False
    use_fuzzy_matching: bool = True
    fuzzy_threshold: float = 0.6
    priority_keywords: Dict[ColumnPriority, List[str]] = field(default_factory=dict)
    table_aliases: Dict[str, List[str]] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize default priorities if not provided"""
        if not self.priority_keywords:
            self.priority_keywords = {
                ColumnPriority.CRITICAL: ['id', 'key', 'pk', 'primary'],
                ColumnPriority.HIGH: [
                    'name', 'title', 'email', 'user', 'date',
                    'created', 'updated', 'modified', 'time'
                ],
                ColumnPriority.MEDIUM: [
                    'status', 'state', 'type', 'category', 'code',
                    'number', 'amount', 'price', 'total', 'count'
                ],
            }
        
        if not self.table_aliases:
            self.table_aliases = {
                'user': ['users', 'accounts', 'customers', 'members'],
                'order': ['orders', 'purchases', 'transactions'],
                'product': ['products', 'items', 'goods'],
                'crime': ['crimes', 'fir', 'cases', 'incidents'],
            }

# ============================================================================
# Table Matcher - Finds relevant tables
# ============================================================================

class TableMatcher:
    """Intelligently matches tables to queries with scoring"""
    
    def __init__(self, config: SchemaConfig):
        self.config = config
    
    def find_relevant_tables(
        self,
        user_message: str,
        available_tables: List[str]
    ) -> List[Tuple[str, float]]:
        """
        Find relevant tables with relevance scores
        
        Returns:
            List of (table_name, score) tuples sorted by relevance
        """
        message_lower = user_message.lower()
        scores = {}
        
        for table in available_tables:
            score = self._calculate_relevance_score(message_lower, table)
            if score > 0:
                scores[table] = score
        
        # Sort by score descending
        sorted_tables = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        # Return top N tables
        return sorted_tables[:self.config.max_tables]
    
    def _calculate_relevance_score(self, message: str, table_name: str) -> float:
        """
        Calculate how relevant a table is to the query
        
        Returns score 0.0 to 1.0
        """
        score = 0.0
        table_lower = table_name.lower()
        
        # Exact match - highest score
        if table_lower in message:
            score = 1.0
        
        # Check table name parts (for names like 'user_accounts')
        table_parts = re.split(r'[_\s]+', table_lower)
        for part in table_parts:
            if len(part) > 2 and part in message:
                score = max(score, 0.8)
        
        # Check aliases
        for key, aliases in self.config.table_aliases.items():
            if key in message:
                if table_lower in aliases or any(a in table_lower for a in aliases):
                    score = max(score, 0.7)
        
        # Fuzzy matching (if enabled)
        if self.config.use_fuzzy_matching and score == 0:
            fuzzy_score = self._fuzzy_match(message, table_lower)
            if fuzzy_score >= self.config.fuzzy_threshold:
                score = fuzzy_score * 0.6  # Scale down fuzzy matches
        
        return score
    
    def _fuzzy_match(self, message: str, table_name: str) -> float:
        """
        Simple fuzzy matching based on character overlap
        
        Returns similarity score 0.0 to 1.0
        """
        # Extract words from message
        message_words = set(re.findall(r'\b\w{3,}\b', message.lower()))
        
        # Check if any word is similar to table name
        max_similarity = 0.0
        
        for word in message_words:
            similarity = self._string_similarity(word, table_name)
            max_similarity = max(max_similarity, similarity)
        
        return max_similarity
    
    @staticmethod
    def _string_similarity(s1: str, s2: str) -> float:
        """Calculate similarity between two strings (0.0 to 1.0)"""
        if not s1 or not s2:
            return 0.0
        
        # Simple character-based similarity
        s1_set = set(s1.lower())
        s2_set = set(s2.lower())
        
        intersection = len(s1_set & s2_set)
        union = len(s1_set | s2_set)
        
        return intersection / union if union > 0 else 0.0

# ============================================================================
# Column Prioritizer - Determines column importance
# ============================================================================

class ColumnPrioritizer:
    """Prioritizes columns based on importance"""
    
    def __init__(self, config: SchemaConfig):
        self.config = config
    
    def prioritize_columns(
        self,
        columns: List[Dict]
    ) -> List[ColumnInfo]:
        """
        Assign priorities to columns
        
        Returns:
            List of ColumnInfo with priorities assigned
        """
        column_infos = []
        
        for col_dict in columns:
            col_info = ColumnInfo.from_dict(col_dict)
            
            # Assign priority based on column name
            col_info.priority = self._determine_priority(col_info.name)
            
            # Mark as key if it looks like a primary key
            col_info.is_key = self._is_likely_key(col_info.name)
            
            column_infos.append(col_info)
        
        return column_infos
    
    def _determine_priority(self, column_name: str) -> ColumnPriority:
        """Determine column priority based on name"""
        col_lower = column_name.lower()
        
        # Check each priority level
        for priority, keywords in self.config.priority_keywords.items():
            if any(keyword in col_lower for keyword in keywords):
                return priority
        
        return ColumnPriority.LOW
    
    @staticmethod
    def _is_likely_key(column_name: str) -> bool:
        """Check if column is likely a key field"""
        col_lower = column_name.lower()
        key_indicators = ['_id', 'id', 'key', 'pk', 'primary']
        return any(indicator in col_lower for indicator in key_indicators)

# ============================================================================
# Schema Formatter - Formats schema for display
# ============================================================================

class SchemaFormatter:
    """Formats schema information for LLM consumption"""
    
    def __init__(self, config: SchemaConfig):
        self.config = config
    
    def format_compact(
        self,
        tables: List[TableInfo],
        database_type: str = "V2 Data"
    ) -> str:
        """
        Format tables in compact format
        
        Args:
            tables: List of TableInfo objects
            database_type: "V2 Data" (PostgreSQL) or "V1 Data" (MongoDB)
        
        Returns:
            Compact formatted string
        """
        if not tables:
            return ""
        
        # Add explicit field name warning
        if database_type == "V2 Data":
            output = [f"{database_type} (PostgreSQL - use lowercase column names):"]
        elif database_type == "V1 Data":
            output = [f"{database_type} (MongoDB - use UPPERCASE field names):"]
        else:
            output = [f"{database_type}:"]
        
        for table in tables:
            key_cols = table.get_key_columns(self.config.max_columns)
            
            # Format column list
            if self.config.show_data_types:
                col_strs = [f"{c.name}({c.data_type})" for c in key_cols]
            else:
                col_strs = [c.name for c in key_cols]
            
            col_list = ', '.join(col_strs)
            
            # Add table line
            output.append(f"  {table.name}: {col_list}")
            
            # Show if more columns exist
            remaining = len(table.columns) - len(key_cols)
            if remaining > 0:
                output.append(f"    (+{remaining} more columns)")
        
        return "\n".join(output)

# ============================================================================
# Main Smart Schema Selector
# ============================================================================

class SmartSchemaSelector:
    """
    Intelligently selects relevant schema based on user queries
    """
    
    def __init__(self, config: Optional[SchemaConfig] = None):
        """
        Initialize schema selector
        
        Args:
            config: Optional configuration (uses defaults if None)
        """
        self.config = config or SchemaConfig()
        self.table_matcher = TableMatcher(self.config)
        self.column_prioritizer = ColumnPrioritizer(self.config)
        self.formatter = SchemaFormatter(self.config)
    
    def get_targeted_schema(
        self,
        schema_dict: Dict[str, Any],
        relevant_tables: Dict[str, List[str]],
        max_columns: int = 10
    ) -> str:
        """
        Get schema for specific relevant tables (most intelligent method)
        
        Args:
            schema_dict: Full schema dictionary
            relevant_tables: {'postgresql': [tables], 'mongodb': [collections]}
            max_columns: Maximum columns per table
        
        Returns:
            Compact schema string with only relevant tables
        """
        # Update max_columns in config temporarily
        original_max = self.config.max_columns
        self.config.max_columns = max_columns
        
        output = []
        
        # Process PostgreSQL tables (V2 Data)
        if relevant_tables.get('postgresql') and schema_dict.get('postgresql'):
            pg_tables = self._process_tables(
                schema_dict['postgresql'],
                relevant_tables['postgresql']
            )
            if pg_tables:
                output.append(self.formatter.format_compact(pg_tables, "V2 Data"))
        
        # Process MongoDB collections (V1 Data)
        if relevant_tables.get('mongodb') and schema_dict.get('mongodb'):
            mongo_tables = self._process_mongo_collections(
                schema_dict['mongodb'],
                relevant_tables['mongodb']
            )
            if mongo_tables:
                if output:
                    output.append("")
                output.append(self.formatter.format_compact(mongo_tables, "V1 Data"))
        
        # Restore original config
        self.config.max_columns = original_max
        
        # If no relevant tables, show compact schema
        if not output:
            return self.get_compact_schema(schema_dict, "", max_columns, max_tables=3)
        
        return "\n".join(output)
    
    def get_compact_schema(
        self,
        schema_dict: Dict[str, Any],
        user_message: str = "",
        max_columns: int = 8,
        max_tables: int = 3
    ) -> str:
        """
        Get ultra-compact schema with intelligent table selection
        
        Args:
            schema_dict: Full schema dictionary
            user_message: User's query for context
            max_columns: Maximum columns per table
            max_tables: Maximum tables to include
        
        Returns:
            Compact schema string
        """
        output = []
        
        # PostgreSQL
        if schema_dict.get('postgresql'):
            pg_table_names = list(schema_dict['postgresql'].keys())
            
            # Find relevant tables
            if user_message:
                relevant = self.table_matcher.find_relevant_tables(
                    user_message,
                    pg_table_names
                )
                selected_tables = [name for name, score in relevant[:max_tables]]
            else:
                selected_tables = pg_table_names[:max_tables]
            
            # Process tables
            pg_tables = self._process_tables(
                schema_dict['postgresql'],
                selected_tables
            )
            
            if pg_tables:
                # Temporarily update config
                original_max = self.config.max_columns
                self.config.max_columns = max_columns
                
                output.append(self.formatter.format_compact(pg_tables, "V2 Data"))
                
                self.config.max_columns = original_max
                
                # Show total if limited
                if len(selected_tables) < len(pg_table_names):
                    output.append(f"  ... +{len(pg_table_names) - len(selected_tables)} more tables")
        
        # MongoDB
        if schema_dict.get('mongodb'):
            mongo_collection_names = list(schema_dict['mongodb'].keys())
            
            # Find relevant collections
            if user_message:
                relevant = self.table_matcher.find_relevant_tables(
                    user_message,
                    mongo_collection_names
                )
                selected_collections = [name for name, score in relevant[:max_tables]]
            else:
                selected_collections = mongo_collection_names[:max_tables]
            
            # Process collections
            mongo_tables = self._process_mongo_collections(
                schema_dict['mongodb'],
                selected_collections
            )
            
            if mongo_tables:
                if output:
                    output.append("")
                
                # Temporarily update config
                original_max = self.config.max_columns
                self.config.max_columns = max_columns
                
                output.append(self.formatter.format_compact(mongo_tables, "V1 Data"))
                
                self.config.max_columns = original_max
                
                # Show total if limited
                if len(selected_collections) < len(mongo_collection_names):
                    output.append(f"  ... +{len(mongo_collection_names) - len(selected_collections)} more collections")
        
        return "\n".join(output)
    
    def _process_tables(
        self,
        schema_tables: Dict[str, List[Dict]],
        table_names: List[str]
    ) -> List[TableInfo]:
        """Process PostgreSQL tables into TableInfo objects"""
        tables = []
        
        for table_name in table_names:
            if table_name not in schema_tables:
                continue
            
            columns = schema_tables[table_name]
            column_infos = self.column_prioritizer.prioritize_columns(columns)
            
            table_info = TableInfo(
                name=table_name,
                columns=column_infos
            )
            
            tables.append(table_info)
        
        return tables
    
    def _process_mongo_collections(
        self,
        schema_collections: Dict[str, Dict],
        collection_names: List[str]
    ) -> List[TableInfo]:
        """Process MongoDB collections into TableInfo objects"""
        tables = []
        
        for collection_name in collection_names:
            if collection_name not in schema_collections:
                continue
            
            collection_info = schema_collections[collection_name]
            fields = collection_info.get('fields', [])
            
            # Convert MongoDB fields to column format
            columns = [
                {
                    'column': field['field'],
                    'type': field['type'],
                    'nullable': True
                }
                for field in fields
            ]
            
            column_infos = self.column_prioritizer.prioritize_columns(columns)
            
            table_info = TableInfo(
                name=collection_name,
                columns=column_infos,
                row_count=collection_info.get('count')
            )
            
            tables.append(table_info)
        
        return tables
    
    # Backward compatibility methods
    @staticmethod
    def extract_relevant_tables(
        user_message: str,
        all_tables: List[str],
        max_tables: int = 3
    ) -> List[str]:
        """
        Backward compatible method
        Extract table names from user message
        """
        selector = SmartSchemaSelector(SchemaConfig(max_tables=max_tables))
        relevant = selector.table_matcher.find_relevant_tables(
            user_message,
            all_tables
        )
        return [name for name, score in relevant]

# ============================================================================
# Utility Functions
# ============================================================================

def create_schema_selector(
    max_tables: int = 3,
    max_columns: int = 10,
    show_types: bool = True,
    use_fuzzy: bool = True
) -> SmartSchemaSelector:
    """
    Factory function to create configured SmartSchemaSelector
    
    Args:
        max_tables: Maximum tables to show
        max_columns: Maximum columns per table
        show_types: Include data types in output
        use_fuzzy: Enable fuzzy matching
    
    Returns:
        Configured SmartSchemaSelector instance
    """
    config = SchemaConfig(
        max_tables=max_tables,
        max_columns=max_columns,
        show_data_types=show_types,
        use_fuzzy_matching=use_fuzzy
    )
    return SmartSchemaSelector(config)


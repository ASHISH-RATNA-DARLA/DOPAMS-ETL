"""
Intelligent Query Planner - Professional Version
Analyzes user questions and plans the best way to answer them
"""

import re
import logging
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache

logger = logging.getLogger(__name__)

# ============================================================================
# Enums for better type safety
# ============================================================================

class QueryIntent(Enum):
    """User query intent types"""
    COUNT_AGGREGATE = "count_aggregate"
    RETRIEVE_DATA = "retrieve_data"
    SEARCH_FILTER = "search_filter"
    SCHEMA_INFO = "schema_info"
    COMPARISON = "comparison"
    GET_DETAILS = "get_details"
    GENERAL_QUERY = "general_query"

class TimeFilter(Enum):
    """Time filter types"""
    TODAY = "today"
    YESTERDAY = "yesterday"
    THIS_WEEK = "week"
    LAST_WEEK = "last_week"
    THIS_MONTH = "month"
    LAST_MONTH = "last_month"
    THIS_YEAR = "year"
    RECENT = "recent"

# ============================================================================
# Data classes for structured results
# ============================================================================

@dataclass
class QueryPlan:
    """Structured query plan result"""
    intent: QueryIntent
    relevant_tables: Dict[str, List[str]]
    search_terms: List[str] = field(default_factory=list)
    needs_aggregation: bool = False
    needs_join: bool = False
    time_filter: Optional[TimeFilter] = None
    limit: int = 100
    confidence: float = 1.0
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for backwards compatibility"""
        return {
            'intent': self.intent.value,
            'relevant_tables': self.relevant_tables,
            'search_terms': self.search_terms,
            'needs_aggregation': self.needs_aggregation,
            'needs_join': self.needs_join,
            'time_filter': self.time_filter.value if self.time_filter else None,
            'limit': self.limit,
            'confidence': self.confidence,
            'metadata': self.metadata
        }

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class PlannerConfig:
    """Configuration for query planner"""
    default_limit: int = 100
    max_limit: int = 10000
    enable_caching: bool = True
    custom_aliases: Dict[str, List[str]] = field(default_factory=dict)
    
    def get_all_aliases(self) -> Dict[str, List[str]]:
        """Get combined default and custom aliases"""
        default_aliases = {
            'fir': ['crime', 'case', 'report', 'complaint', 'incident'],
            'user': ['account', 'person', 'customer', 'member'],
            'order': ['purchase', 'transaction', 'sale', 'checkout'],
            'product': ['item', 'goods', 'merchandise'],
            'crime': ['fir', 'case', 'incident', 'offense'],
            'record': ['entry', 'data', 'document', 'row'],
            'log': ['event', 'activity', 'audit'],
            'customer': ['user', 'client', 'buyer'],
        }
        # Merge with custom aliases
        return {**default_aliases, **self.custom_aliases}

# ============================================================================
# Intent Detection
# ============================================================================

class IntentDetector:
    """Detects user intent from natural language"""
    
    # Intent keyword mappings
    INTENT_KEYWORDS = {
        QueryIntent.COUNT_AGGREGATE: [
            'count', 'how many', 'total', 'sum', 'average', 'avg',
            'max', 'min', 'statistics', 'stats', 'aggregate'
        ],
        QueryIntent.RETRIEVE_DATA: [
            'show', 'list', 'get', 'find', 'display', 'give me',
            'fetch', 'retrieve', 'view'
        ],
        QueryIntent.SEARCH_FILTER: [
            'search', 'find where', 'with', 'containing', 'filter',
            'where', 'matching'
        ],
        QueryIntent.SCHEMA_INFO: [
            'what tables', 'what collections', 'schema', 'structure',
            'columns', 'fields', 'available'
        ],
        QueryIntent.COMPARISON: [
            'compare', 'difference', 'vs', 'versus', 'between'
        ],
        QueryIntent.GET_DETAILS: [
            'details', 'more info', 'information about', 'about',
            'describe'
        ]
    }
    
    @staticmethod
    def detect(message: str) -> Tuple[QueryIntent, float]:
        """
        Detect intent with confidence score
        
        Returns:
            Tuple of (QueryIntent, confidence_score)
        """
        message_lower = message.lower()
        scores = {}
        
        for intent, keywords in IntentDetector.INTENT_KEYWORDS.items():
            matches = sum(1 for kw in keywords if kw in message_lower)
            if matches > 0:
                scores[intent] = matches / len(keywords)
        
        if scores:
            best_intent = max(scores.items(), key=lambda x: x[1])
            return best_intent[0], best_intent[1]
        
        return QueryIntent.GENERAL_QUERY, 0.5

# ============================================================================
# Table Matching
# ============================================================================

class TableMatcher:
    """Intelligently matches tables to user queries"""
    
    def __init__(self, config: PlannerConfig):
        self.config = config
        self._keyword_cache = {}
    
    def find_relevant_tables(
        self,
        message: str,
        available_tables: Dict[str, Dict]
    ) -> Dict[str, List[str]]:
        """
        Find relevant tables based on keywords and context
        
        Args:
            message: User query
            available_tables: Dict with 'postgresql' and/or 'mongodb' keys
        
        Returns:
            Dict: {'postgresql': [tables], 'mongodb': [collections]}
        """
        relevant = {'postgresql': [], 'mongodb': []}
        message_lower = message.lower()
        
        # PostgreSQL tables
        if available_tables.get('postgresql'):
            relevant['postgresql'] = self._match_tables(
                message_lower,
                list(available_tables['postgresql'].keys())
            )
        
        # MongoDB collections
        if available_tables.get('mongodb'):
            relevant['mongodb'] = self._match_tables(
                message_lower,
                list(available_tables['mongodb'].keys())
            )
        
        return relevant
    
    def _match_tables(self, message: str, table_names: List[str]) -> List[str]:
        """Match tables against message"""
        matched = []
        
        for table_name in table_names:
            # Direct mention
            if table_name.lower() in message:
                matched.append(table_name)
                continue
            
            # Keyword matching
            keywords = self._get_table_keywords(table_name)
            if any(keyword in message for keyword in keywords):
                matched.append(table_name)
        
        return matched
    
    @lru_cache(maxsize=256)
    def _get_table_keywords(self, table_name: str) -> Tuple[str, ...]:
        """
        Generate search keywords from table name (cached)
        
        Returns tuple for hashability/caching
        """
        keywords = set()
        
        # Split by underscore and camelCase
        parts = re.split(r'[_\s]+|(?<=[a-z])(?=[A-Z])', table_name.lower())
        keywords.update(parts)
        
        # Add singular/plural variations
        for part in parts:
            if len(part) > 2:  # Skip very short words
                if part.endswith('s'):
                    keywords.add(part[:-1])
                else:
                    keywords.add(part + 's')
        
        # Add aliases from config
        aliases = self.config.get_all_aliases()
        for part in parts:
            if part in aliases:
                keywords.update(aliases[part])
        
        return tuple(keywords)  # Return tuple for caching

# ============================================================================
# Feature Extractors
# ============================================================================

class FeatureExtractor:
    """Extracts various features from user queries"""
    
    @staticmethod
    def extract_search_terms(message: str) -> List[str]:
        """Extract specific search terms (IDs, names, values)"""
        search_terms = []
        
        # MongoDB ObjectIds
        objectid_pattern = r'\b[a-f0-9]{24}\b'
        search_terms.extend(re.findall(objectid_pattern, message, re.IGNORECASE))
        
        # Numeric IDs (4+ digits)
        numbers = re.findall(r'\b\d{4,}\b', message)
        search_terms.extend(numbers)
        
        # Quoted strings
        search_terms.extend(re.findall(r'"([^"]+)"', message))
        search_terms.extend(re.findall(r"'([^']+)'", message))
        
        return list(set(search_terms))  # Remove duplicates
    
    @staticmethod
    def needs_aggregation(message: str) -> bool:
        """Check if query needs aggregation"""
        agg_keywords = [
            'count', 'sum', 'total', 'average', 'avg', 'max', 'min',
            'group by', 'grouped', 'statistics', 'stats'
        ]
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in agg_keywords)
    
    @staticmethod
    def needs_join(message: str) -> bool:
        """Check if query needs JOIN"""
        join_keywords = [
            'with', 'and their', 'along with', 'including',
            'combined', 'together with'
        ]
        message_lower = message.lower()
        return any(keyword in message_lower for keyword in join_keywords)
    
    @staticmethod
    def extract_time_filter(message: str) -> Optional[TimeFilter]:
        """Extract time-related filters"""
        message_lower = message.lower()
        
        time_mappings = {
            'today': TimeFilter.TODAY,
            'yesterday': TimeFilter.YESTERDAY,
            'this week': TimeFilter.THIS_WEEK,
            'last week': TimeFilter.LAST_WEEK,
            'this month': TimeFilter.THIS_MONTH,
            'last month': TimeFilter.LAST_MONTH,
            'this year': TimeFilter.THIS_YEAR,
            'recent': TimeFilter.RECENT
        }
        
        for keyword, filter_type in time_mappings.items():
            if keyword in message_lower:
                return filter_type
        
        return None
    
    @staticmethod
    def extract_limit(message: str, default: int = 100) -> int:
        """Extract limit/top N from message"""
        message_lower = message.lower()
        
        # Look for explicit numbers
        patterns = [
            (r'top\s+(\d+)', 1),
            (r'first\s+(\d+)', 1),
            (r'last\s+(\d+)', 1),
            (r'latest\s+(\d+)', 1),
            (r'recent\s+(\d+)', 1),
            (r'limit\s+(\d+)', 1)
        ]
        
        for pattern, group in patterns:
            match = re.search(pattern, message_lower)
            if match:
                return int(match.group(group))
        
        # Contextual defaults
        if any(word in message_lower for word in ['recent', 'latest', 'last']):
            return 10
        
        if 'all' in message_lower:
            return 1000
        
        return default

# ============================================================================
# Main Query Planner
# ============================================================================

class IntelligentQueryPlanner:
    """Plan queries intelligently based on user intent"""
    
    def __init__(self, config: Optional[PlannerConfig] = None):
        """
        Initialize query planner
        
        Args:
            config: Optional configuration
        """
        self.config = config or PlannerConfig()
        self.intent_detector = IntentDetector()
        self.table_matcher = TableMatcher(self.config)
        self.feature_extractor = FeatureExtractor()
    
    def analyze_user_question(
        self,
        user_message: str,
        available_tables: Dict[str, Dict]
    ) -> Dict:
        """
        Analyze user question and determine what data is needed
        
        Args:
            user_message: User's natural language question
            available_tables: Dict of available tables/collections
        
        Returns:
            Dict with analysis results (for backwards compatibility)
        """
        message_lower = user_message.lower()
        
        # Detect intent
        intent, confidence = self.intent_detector.detect(message_lower)
        
        # Find relevant tables
        relevant_tables = self.table_matcher.find_relevant_tables(
            message_lower,
            available_tables
        )
        
        # Extract features
        search_terms = self.feature_extractor.extract_search_terms(message_lower)
        needs_agg = self.feature_extractor.needs_aggregation(message_lower)
        needs_join = self.feature_extractor.needs_join(message_lower)
        time_filter = self.feature_extractor.extract_time_filter(message_lower)
        limit = self.feature_extractor.extract_limit(
            message_lower,
            self.config.default_limit
        )
        
        # Create query plan
        plan = QueryPlan(
            intent=intent,
            relevant_tables=relevant_tables,
            search_terms=search_terms,
            needs_aggregation=needs_agg,
            needs_join=needs_join,
            time_filter=time_filter,
            limit=min(limit, self.config.max_limit),
            confidence=confidence,
            metadata={'original_query': user_message}
        )
        
        logger.info(f"Query plan: intent={intent.value}, confidence={confidence:.2f}, tables={relevant_tables}")
        
        # Return dict for backwards compatibility
        return plan.to_dict()
    
    def create_intelligent_prompt(
        self,
        user_message: str,
        schema: str,
        query_plan: Dict
    ) -> str:
        """
        Create an intelligent prompt with context and reasoning
        
        Args:
            user_message: User's question
            schema: Relevant schema
            query_plan: Query plan dict from analysis
        
        Returns:
            Smart prompt for LLM
        """
        context = []
        
        # Intent context
        intent_contexts = {
            'count_aggregate': "User wants COUNT or aggregate statistics.",
            'get_details': "User wants detailed information about specific record(s).",
            'retrieve_data': "User wants to see actual data rows.",
            'search_filter': "User wants filtered/searched results.",
            'comparison': "User wants to compare data."
        }
        
        intent = query_plan.get('intent', 'general_query')
        if intent in intent_contexts:
            context.append(intent_contexts[intent])
        
        # Search terms
        search_terms = query_plan.get('search_terms', [])
        if search_terms:
            context.append(f"Search for: {', '.join(search_terms)}")
        
        # Time filter
        time_filter = query_plan.get('time_filter')
        if time_filter:
            context.append(f"Time filter: {time_filter}")
        
        # Limit
        limit = query_plan.get('limit')
        if limit:
            context.append(f"Limit: {limit} rows")
        
        # Aggregation hint
        if query_plan.get('needs_aggregation'):
            context.append("Use aggregation functions (COUNT, SUM, AVG, etc.)")
        
        # Join hint
        if query_plan.get('needs_join'):
            context.append("May need to JOIN multiple tables")
        
        context_str = "\n".join(f"- {c}" for c in context) if context else "General query"
        
        return f"""{schema}

Context:
{context_str}

User Question: {user_message}

Generate appropriate query:"""

# ============================================================================
# Convenience functions for backwards compatibility
# ============================================================================

_default_planner = None

def get_default_planner() -> IntelligentQueryPlanner:
    """Get or create default planner instance"""
    global _default_planner
    if _default_planner is None:
        _default_planner = IntelligentQueryPlanner()
    return _default_planner


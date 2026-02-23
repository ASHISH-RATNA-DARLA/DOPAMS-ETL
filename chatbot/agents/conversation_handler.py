"""
Conversation Handler - World-Class Natural Language Interface
Intelligent, context-aware conversational responses with personality
"""

import re
import random
import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class ConversationType(Enum):
    """Types of conversation interactions"""
    GREETING = "greeting"
    FAREWELL = "farewell"
    GRATITUDE = "gratitude"
    QUERY = "query"
    CLARIFICATION = "clarification"
    HELP = "help"
    SMALL_TALK = "small_talk"
    UNKNOWN = "unknown"

class UserIntent(Enum):
    """User intent categories"""
    DATA_QUERY = "data_query"
    DATA_COUNT = "data_count"
    SCHEMA_INFO = "schema_info"
    HELP_REQUEST = "help_request"
    CASUAL_CHAT = "casual_chat"
    UNCLEAR = "unclear"

@dataclass
class ConversationContext:
    """Context for maintaining conversation state"""
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    last_query: Optional[str] = None
    last_intent: Optional[UserIntent] = None
    query_count: int = 0
    conversation_history: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    
    def add_message(self, message: str):
        """Add message to history"""
        self.conversation_history.append(message)
        if len(self.conversation_history) > 10:  # Keep last 10
            self.conversation_history.pop(0)

@dataclass
class ConversationResponse:
    """Structured conversation response"""
    message: str
    conversation_type: ConversationType
    requires_clarification: bool = False
    suggestions: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)

# ============================================================================
# Response Templates
# ============================================================================

class ResponseTemplates:
    """Rich response templates with variations"""
    
    GREETINGS = [
        "Hello! 👋 I'm **DOPAMS AI**, your intelligent database assistant. How can I help you explore your data today?",
        "Hi there! I'm **DOPAMS AI**. I can help you query and analyze your databases. What would you like to know?",
        "Hey! 😊 I'm **DOPAMS AI**, ready to dive into your data. Ask me anything!",
        "Welcome! I'm **DOPAMS AI**, here to help you unlock insights from your databases. What are you looking for?",
        "Greetings! I'm **DOPAMS AI**. Let me help you navigate your data. What would you like to explore?",
    ]
    
    FAREWELLS = [
        "Goodbye! Feel free to come back anytime you need to query your data. 👋",
        "See you later! Happy data exploring! 🚀",
        "Take care! I'll be here whenever you need to analyze your data.",
        "Bye! Don't hesitate to return if you have more questions about your data.",
        "Until next time! Hope I was helpful! 😊",
    ]
    
    GRATITUDE_RESPONSES = [
        "You're very welcome! Happy to help! 😊",
        "Glad I could assist! Feel free to ask anything else.",
        "My pleasure! Let me know if you need anything else.",
        "Anytime! That's what I'm here for! 🎯",
        "You're welcome! I'm always ready to help with your data queries.",
    ]
    
    HELP_RESPONSES = [
        """I can help you with:

🔍 **Query Data**
  • "Show me all FIR records from last month"
  • "Count crimes in Sangareddy district"
  • "Find person with mobile number 9876543210"

📊 **Analyze Data**
  • "What's the trend in crime types?"
  • "Show statistics for recent cases"
  • "Analyze crime patterns by location"

ℹ️ **Schema Information**
  • "What tables are available?"
  • "Show me the structure of crimes table"
  • "List all collections"

💡 **Tips:**
  • Be specific about what you want
  • Mention time ranges if relevant
  • Ask follow-up questions to dive deeper

What would you like to explore?""",
        
        """Here are some example queries you can try:

📋 **Simple Queries:**
  • Show me recent FIR records
  • Count total crimes
  • Find all pending cases

🔎 **Detailed Searches:**
  • Find suspects in Sangareddy district
  • Search person with mobile 9876543210
  • Show FIR records from last week

📈 **Analytics:**
  • How many crimes per district?
  • Count crimes by type
  • Show recent drug seizures

What would you like to know?"""
    ]
    
    SCHEMA_REQUESTS = [
        "I can show you the available tables and collections. Which database would you like to explore: PostgreSQL, MongoDB, or both?",
        "Sure! I'll show you what tables and collections are available. One moment...",
        "Let me list the available data structures for you.",
    ]
    
    CLARIFICATION_NEEDED = [
        "I'm not quite sure I understand. Could you rephrase your question?",
        "Hmm, I need a bit more information. Can you be more specific about what you're looking for?",
        "I want to help, but I'm not clear on what you need. Could you elaborate?",
        "Let me make sure I understand correctly. Are you asking about {topic}?",
    ]
    
    AMBIGUOUS_QUERY = [
        "Your query seems a bit broad. Could you be more specific? For example:",
        "I found multiple possible interpretations. Did you mean:",
        "To give you the best results, I need to know more. Are you interested in:",
    ]
    
    NO_RESULTS = [
        "I couldn't find any matching records. Would you like to:",
        "No results found. Here are some suggestions:",
        "Nothing matched your query. You might want to try:",
    ]

# ============================================================================
# Pattern Matchers
# ============================================================================

class PatternMatcher:
    """Advanced pattern matching for conversation understanding"""
    
    # Greeting patterns
    GREETING_PATTERNS = [
        r'\b(hello|hi|hey|greetings|good\s+morning|good\s+afternoon|good\s+evening)\b',
        r'^(yo|sup|howdy|hiya)\b',
    ]
    
    # Farewell patterns
    FAREWELL_PATTERNS = [
        r'\b(bye|goodbye|see\s+you|farewell|later|exit|quit)\b',
        r'\b(good\s+night|take\s+care)\b',
    ]
    
    # Gratitude patterns
    GRATITUDE_PATTERNS = [
        r'\b(thank|thanks|thx|appreciate|grateful)\b',
    ]
    
    # Help request patterns
    HELP_PATTERNS = [
        r'\b(help|assist|how\s+do|what\s+can|guide|tutorial)\b',
        r'\b(confused|lost|don\'t\s+understand)\b',
    ]
    
    # Schema information patterns
    SCHEMA_PATTERNS = [
        r'\b(what\s+tables|show\s+tables|list\s+tables|available\s+tables)\b',
        r'\b(what\s+collections|show\s+collections|list\s+collections)\b',
        r'\b(database\s+structure|schema|table\s+structure)\b',
        r'\b(what\s+data|what\s+is\s+available|what\s+do\s+you\s+have)\b',
    ]
    
    # Query intent patterns
    QUERY_PATTERNS = [
        r'\b(show|display|get|fetch|find|search|list|give\s+me)\b',
        r'\b(count|how\s+many|number\s+of|total)\b',
        r'\b(select|query|retrieve)\b',
    ]
    
    # Ambiguous indicators
    AMBIGUOUS_INDICATORS = [
        r'^(something|anything|everything|some|any|all)$',
        r'^(stuff|things|data|records)$',
    ]
    
    @classmethod
    @lru_cache(maxsize=256)
    def matches_pattern(cls, text: str, pattern: str) -> bool:
        """Check if text matches pattern (cached)"""
        return bool(re.search(pattern, text, re.IGNORECASE))
    
    @classmethod
    def matches_any_pattern(cls, text: str, patterns: List[str]) -> bool:
        """Check if text matches any pattern in list"""
        return any(cls.matches_pattern(text, p) for p in patterns)

# ============================================================================
# Context Analyzer
# ============================================================================

class ContextAnalyzer:
    """Analyzes conversation context for better responses"""
    
    @staticmethod
    def detect_conversation_type(message: str) -> ConversationType:
        """Detect the type of conversation"""
        message_lower = message.lower().strip()
        
        # Check greetings
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.GREETING_PATTERNS):
            return ConversationType.GREETING
        
        # Check farewells
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.FAREWELL_PATTERNS):
            return ConversationType.FAREWELL
        
        # Check gratitude
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.GRATITUDE_PATTERNS):
            return ConversationType.GRATITUDE
        
        # Check help requests
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.HELP_PATTERNS):
            return ConversationType.HELP
        
        # Check queries
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.QUERY_PATTERNS):
            return ConversationType.QUERY
        
        # Check if asking for clarification
        if any(word in message_lower for word in ['what do you mean', 'explain', 'clarify']):
            return ConversationType.CLARIFICATION
        
        # Check small talk
        if len(message_lower.split()) <= 5 and '?' not in message:
            return ConversationType.SMALL_TALK
        
        return ConversationType.UNKNOWN
    
    @staticmethod
    def detect_user_intent(message: str, available_schema: Dict) -> UserIntent:
        """Detect detailed user intent"""
        message_lower = message.lower()
        
        # Schema information request
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.SCHEMA_PATTERNS):
            return UserIntent.SCHEMA_INFO
        
        # Help request
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.HELP_PATTERNS):
            return UserIntent.HELP_REQUEST
        
        # Count query
        if any(word in message_lower for word in ['count', 'how many', 'total', 'number of']):
            return UserIntent.DATA_COUNT
        
        # Data query
        if PatternMatcher.matches_any_pattern(message_lower, PatternMatcher.QUERY_PATTERNS):
            return UserIntent.DATA_QUERY
        
        # Unclear intent
        return UserIntent.UNCLEAR
    
    @staticmethod
    def is_ambiguous(message: str) -> bool:
        """Check if message is too ambiguous"""
        message_lower = message.lower().strip()
        
        # Very short messages without specific terms
        if len(message_lower.split()) <= 2:
            if not any(char.isdigit() for char in message_lower):
                if not PatternMatcher.matches_any_pattern(
                    message_lower,
                    PatternMatcher.QUERY_PATTERNS
                ):
                    return True
        
        # Matches ambiguous indicators
        if PatternMatcher.matches_any_pattern(
            message_lower,
            PatternMatcher.AMBIGUOUS_INDICATORS
        ):
            return True
        
        return False

# ============================================================================
# Relevance Checker
# ============================================================================

class RelevanceChecker:
    """Checks if user query is relevant to available data"""
    
    # Data-related keywords
    DATA_KEYWORDS = [
        'show', 'get', 'find', 'search', 'list', 'display', 'fetch',
        'count', 'how many', 'total', 'sum', 'average',
        'select', 'query', 'retrieve', 'data', 'records',
        'table', 'collection', 'database', 'fir', 'crime',
        'crimes', 'cases', 'suspect', 'person', 'accused'
    ]
    
    # Non-data questions
    NON_DATA_QUESTIONS = [
        r'\b(what\s+is\s+your\s+name|who\s+are\s+you|what\s+can\s+you\s+do)\b',
        r'\b(weather|news|time|date|joke)\b',
        r'\b(how\s+are\s+you|how\'s\s+it\s+going)\b',
    ]
    
    @classmethod
    def is_relevant_to_data(
        cls,
        message: str,
        available_schema: Dict
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if message is relevant to data (includes entity detection!)
        
        Returns:
            Tuple of (is_relevant, clarification_message)
        """
        message_lower = message.lower()
        
        # Check for non-data questions
        if PatternMatcher.matches_any_pattern(message_lower, cls.NON_DATA_QUESTIONS):
            return False, cls._get_redirect_message()
        
        # CRITICAL: Check if message contains detectable entities (names, mobiles, IDs, etc.)
        # This handles queries like "9398922883" or "who is Rajendra Prasad"
        try:
            from agents.entity_detector import EntityDetector
            entities = EntityDetector.detect_entities(message)
            if entities:
                # Has person name, mobile, FIR number, etc. → RELEVANT!
                return True, None
        except:
            pass  # Entity detection failed, continue with keyword check
        
        # Check for data-related keywords
        has_data_keyword = any(keyword in message_lower for keyword in cls.DATA_KEYWORDS)
        
        # Check if mentions any table/collection
        mentions_table = cls._mentions_table(message_lower, available_schema)
        
        if has_data_keyword or mentions_table:
            return True, None
        
        # Check if it's a question
        if '?' in message:
            # Generic question without data keywords
            return False, cls._get_suggestion_message()
        
        # Too vague
        return False, cls._get_suggestion_message()
    
    @staticmethod
    def _mentions_table(message: str, schema: Dict) -> bool:
        """Check if message mentions any table/collection name"""
        if not schema:
            return False
        
        message_lower = message.lower()
        
        # Check PostgreSQL tables
        if 'postgresql' in schema:
            for table in schema['postgresql'].keys():
                if table.lower() in message_lower:
                    return True
        
        # Check MongoDB collections
        if 'mongodb' in schema:
            for collection in schema['mongodb'].keys():
                if collection.lower() in message_lower:
                    return True
        
        return False
    
    @staticmethod
    def _get_redirect_message() -> str:
        """Get redirect message for non-data questions"""
        return (
            "I'm **DOPAMS AI**, specialized in helping you query and analyze your databases. "
            "I can't answer general questions, but I'm great at exploring your data! "
            "Try asking: 'Show me all crimes' or 'Count recent FIR records'"
        )
    
    @staticmethod
    def _get_suggestion_message() -> str:
        """Get suggestion message for vague queries"""
        return (
            "I can help you query your databases! Try something specific like:\n"
            "  • 'Show me FIR records from last month'\n"
            "  • 'Count crimes by district'\n"
            "  • 'Find suspects with multiple cases'\n\n"
            "What would you like to explore?"
        )

# ============================================================================
# Main Conversation Handler
# ============================================================================

class ConversationHandler:
    """
    World-class conversation handler with intelligence and personality
    """
    
    def __init__(self, personality: str = "friendly"):
        """
        Initialize conversation handler
        
        Args:
            personality: Conversation personality (friendly, professional, casual)
        """
        self.personality = personality
        self.context_analyzer = ContextAnalyzer()
        self.relevance_checker = RelevanceChecker()
        self.templates = ResponseTemplates()
        
        # Context storage (in production, use proper session management)
        self._contexts: Dict[str, ConversationContext] = {}
    
    def handle_greeting(self, message: str) -> Optional[str]:
        """
        Handle greeting messages
        
        Args:
            message: User message
        
        Returns:
            Greeting response or None if not a greeting
        """
        conv_type = self.context_analyzer.detect_conversation_type(message)
        
        if conv_type == ConversationType.GREETING:
            response = random.choice(self.templates.GREETINGS)
            logger.info("Greeting detected and handled")
            return response
        
        return None
    
    def handle_farewell(self, message: str) -> Optional[str]:
        """Handle farewell messages"""
        conv_type = self.context_analyzer.detect_conversation_type(message)
        
        if conv_type == ConversationType.FAREWELL:
            return random.choice(self.templates.FAREWELLS)
        
        return None
    
    def handle_gratitude(self, message: str) -> Optional[str]:
        """Handle thank you messages"""
        conv_type = self.context_analyzer.detect_conversation_type(message)
        
        if conv_type == ConversationType.GRATITUDE:
            return random.choice(self.templates.GRATITUDE_RESPONSES)
        
        return None
    
    def handle_help_request(self, message: str) -> Optional[str]:
        """Handle help requests"""
        conv_type = self.context_analyzer.detect_conversation_type(message)
        
        if conv_type == ConversationType.HELP:
            return random.choice(self.templates.HELP_RESPONSES)
        
        return None
    
    def is_relevant_to_data(
        self,
        message: str,
        available_schema: Dict
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if message is relevant to data queries
        
        Args:
            message: User message
            available_schema: Available database schema
        
        Returns:
            Tuple of (is_relevant, clarification_message)
        """
        return self.relevance_checker.is_relevant_to_data(message, available_schema)
    
    def clarify_ambiguous_query(
        self,
        message: str,
        available_schema: Dict
    ) -> Optional[str]:
        """
        Check if query needs clarification
        
        Args:
            message: User message
            available_schema: Available database schema
        
        Returns:
            Clarification message or None if query is clear
        """
        if self.context_analyzer.is_ambiguous(message):
            # Build suggestions based on available schema
            suggestions = self._build_suggestions(available_schema)
            
            clarification = random.choice(self.templates.AMBIGUOUS_QUERY)
            clarification += "\n" + "\n".join(f"  • {s}" for s in suggestions[:3])
            
            logger.info("Ambiguous query detected, requesting clarification")
            return clarification
        
        return None
    
    def format_conversational_response(
        self,
        results: Dict,
        user_message: str
    ) -> str:
        """
        Format results with conversational intro
        
        Args:
            results: Query results
            user_message: Original user message
        
        Returns:
            Conversational response intro
        """
        if not results:
            return "I couldn't find any matching data. Would you like to try a different query?"
        
        # Detect intent to customize response
        intent = self.context_analyzer.detect_user_intent(user_message, {})
        
        # Count query
        if intent == UserIntent.DATA_COUNT:
            return "Here are the counts you requested:"
        
        # Schema info
        if intent == UserIntent.SCHEMA_INFO:
            return "Here's what I found in your database schema:"
        
        # Regular query
        total_count = sum(len(data) for data in results.values())
        
        if total_count == 0:
            return "I found no matching records."
        elif total_count == 1:
            return "I found 1 record that matches your query:"
        elif total_count <= 10:
            return f"I found {total_count} records:"
        else:
            return f"I found {total_count} records. Here are the highlights:"
    
    def format_no_results(self, user_message: str, schema: Dict) -> str:
        """Format helpful message when no results found"""
        suggestions = self._build_suggestions(schema)
        
        response = random.choice(self.templates.NO_RESULTS)
        response += "\n" + "\n".join(f"  • {s}" for s in suggestions[:3])
        
        return response
    
    def _build_suggestions(self, schema: Dict) -> List[str]:
        """Build query suggestions based on available schema"""
        suggestions = []
        
        # Add table-based suggestions
        if schema and 'postgresql' in schema:
            tables = list(schema['postgresql'].keys())[:3]
            for table in tables:
                suggestions.append(f"Show me all {table}")
                suggestions.append(f"Count total {table}")
        
        if schema and 'mongodb' in schema:
            collections = list(schema['mongodb'].keys())[:3]
            for collection in collections:
                suggestions.append(f"List {collection}")
        
        # Add time-based suggestions
        suggestions.extend([
            "Show recent data from last week",
            "Find records from this month",
        ])
        
        return suggestions
    
    def get_context(self, session_id: str) -> ConversationContext:
        """Get or create conversation context"""
        if session_id not in self._contexts:
            self._contexts[session_id] = ConversationContext(session_id=session_id)
        return self._contexts[session_id]
    
    def clear_context(self, session_id: str):
        """Clear conversation context"""
        if session_id in self._contexts:
            del self._contexts[session_id]
    
    # ========================================================================
    # Class Methods for Backward Compatibility (used by existing nodes.py)
    # ========================================================================
    
    @classmethod
    def handle_greeting_classmethod(cls, message: str) -> Optional[str]:
        """Handle greeting (class method for backward compatibility)"""
        handler = cls()
        conv_type = handler.context_analyzer.detect_conversation_type(message)
        
        if conv_type == ConversationType.GREETING:
            return random.choice(handler.templates.GREETINGS)
        return None
    
    @classmethod
    def is_relevant_to_data_classmethod(cls, user_message: str, available_tables: Dict) -> Tuple[bool, Optional[str]]:
        """Check relevance (class method for backward compatibility)"""
        handler = cls()
        return handler.relevance_checker.is_relevant_to_data(user_message, available_tables)


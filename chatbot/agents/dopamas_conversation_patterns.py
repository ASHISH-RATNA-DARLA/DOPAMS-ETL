"""
DOPAMAS Conversational Patterns
Comprehensive conversation handling adapted for crime investigation chatbot
"""

import logging
import re
from typing import Dict, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class ConversationType(Enum):
    """Types of conversational interactions"""
    GREETING = "greeting"
    FAREWELL = "farewell"
    SMALL_TALK = "small_talk"
    ACKNOWLEDGMENT = "acknowledgment"
    GRATITUDE = "gratitude"
    FRUSTRATION = "frustration"
    CONFUSION = "confusion"
    DISAPPOINTMENT = "disappointment"
    CLARIFICATION = "clarification"
    FORMAT_PREFERENCE = "format_preference"
    ERROR_REPORT = "error_report"
    EDGE_CASE = "edge_case"
    DATA_QUERY = "data_query"


class DOPAMASConversationPatterns:
    """
    Comprehensive conversation pattern handler for DOPAMAS chatbot
    Adapted for crime investigation domain
    """
    
    # =========================================================================
    # 1. GREETINGS & FAREWELLS (DOPAMAS Context)
    # =========================================================================
    
    GREETINGS = {
        r'\b(hey|hi|hello|hola|namaste)\b': [
            "Hello! I'm the DOPAMAS AI assistant. I can help you search crime records, FIR details, accused information, and more. What would you like to know?",
            "Hi! I can help you with crime data, person searches, drug seizure information, and case statistics. How can I assist you?",
            "Hey! Looking for crime records or person information? I'm here to help!"
        ],
        r'\bgood\s+(morning|morinig|mornig|mornin)\b': [  # Handle common typos
            "Good morning! Ready to help you with crime investigations and data analysis. What do you need?",
            "Good morning! How can I assist you with crime records today?"
        ],
        r'\bgood\s+afternoon\b': [
            "Good afternoon! Need help finding crime data or person information?",
            "Good afternoon! What crime-related information can I help you find?"
        ],
        r'\bgood\s+evening\b': [
            "Good evening! How can I help you with your investigation today?",
            "Good evening! Looking for crime records or statistics?"
        ],
        r'\b(what\'s up|whats up|wassup)\b': [
            "Ready to help you search DOPAMAS crime database! What information do you need?",
            "I'm here to help with crime investigations! What can I find for you?"
        ]
    }
    
    FAREWELLS = {
        r'\b(bye|goodbye|see you|take care|gotta go)\b': [
            "Goodbye! Come back anytime you need crime data or investigation support.",
            "Take care! Feel free to return if you need any crime information.",
            "See you later! I'm here 24/7 for crime data searches."
        ],
        r'\bgood\s+night\b': [
            "Good night! The DOPAMAS system is available anytime you need it.",
            "Good night! Sleep well, and come back anytime for crime data."
        ],
        r'\btalk to you (later|soon)\b': [
            "Sounds good! Come back anytime you need investigation support.",
            "Sure thing! I'll be here when you need crime data."
        ]
    }
    
    # =========================================================================
    # 2. SMALL TALK & ACKNOWLEDGMENTS (DOPAMAS Context)
    # =========================================================================
    
    SMALL_TALK = {
        r'\bhow\s+are\s+you\b': [  # Flexible pattern - handles "how are you", "how are you?", etc.
            "I'm functioning well, thank you! Ready to help you search crime records. What do you need?",
            "I'm doing great! How can I assist you with crime investigations today?"
        ],
        r'\bhow\'s it going\b': [
            "Going well! Ready to help you find crime data. What information do you need?",
            "All systems operational! What crime information can I search for you?"
        ],
        r'\byou there\b': [
            "Yes, I'm here! What crime data do you need?",
            "I'm here and ready! What can I search for you?"
        ]
    }
    
    ACKNOWLEDGMENTS = {
        r'\b(ok|okay|got it|understood|alright|fine)\b': [  # Removed $ to match anywhere in message
            "Great! Anything else you need from the crime database?",
            "Perfect! What other information can I find for you?",
            "Understood! Let me know if you need more data."
        ],
        r'\b(thanks|thank you|thx|ty)\b': [
            "You're welcome! Happy to help with crime investigations.",
            "My pleasure! Come back anytime you need crime data.",
            "Glad I could help! Feel free to ask for more information."
        ],
        r'\b(yes|yeah|yep|sure)\b$': [
            "Great! What would you like to know?",
            "Perfect! How can I help further?",
            "Okay! What's your next question?"
        ],
        r'\b(no|nope|nah)\b$': [
            "Okay! Let me know if you change your mind or need anything else.",
            "No problem! I'm here if you need crime data later."
        ]
    }
    
    # =========================================================================
    # 3. GRATITUDE & POSITIVE EMOTIONS (DOPAMAS Context)
    # =========================================================================
    
    GRATITUDE = {
        r'\bthank you so much\b': [
            "You're very welcome! Happy to help you find crime information.",
            "My pleasure! That's what I'm here for - searching DOPAMAS data efficiently."
        ],
        r'\b(perfect|exactly|that\'s it|yes that\'s it)\b': [
            "Wonderful! Let me know if you need more crime data.",
            "Great! Anything else you'd like to search for?",
            "Excellent! Feel free to ask for additional information."
        ],
        r'\b(amazing|awesome|great job|well done|excellent)\b': [
            "Thank you! Glad I could find the right crime information for you.",
            "Happy to help! Let me know what else you need."
        ],
        r'\byou (saved me|helped so much|are the best)\b': [
            "That's wonderful to hear! I'm here whenever you need investigation support.",
            "Glad I could help! Come back anytime for crime data searches."
        ]
    }
    
    # =========================================================================
    # 4. FRUSTRATION & NEGATIVE EMOTIONS (DOPAMAS Context)
    # =========================================================================
    
    FRUSTRATION = {
        r'\b(not working|doesn\'t work|isn\'t working)\b': [
            "I understand that's frustrating. Let me help you troubleshoot. What specific information were you trying to find?",
            "I'm sorry you're having trouble. Can you tell me what search you were attempting? I'll help fix it."
        ],
        r'\b(you\'re not understanding|you don\'t get it|you\'re missing)\b': [
            "I apologize for the confusion. Let me try again - please tell me exactly what crime information you're looking for.",
            "I'm sorry I misunderstood. Could you rephrase what you need? For example: 'Find FIR 243/2022' or 'Show persons in Sangareddy'?"
        ],
        r'\b(useless|terrible|waste of time|not helpful)\b': [
            "I'm sorry I haven't been helpful. Let me try a different approach. What specific crime data do you need?",
            "I apologize for not meeting your needs. Tell me what you're investigating and I'll search more effectively."
        ],
        r'\b(ugh|seriously|annoyed|frustrated)\b': [
            "I can tell this is frustrating. Let's solve this together. What information do you need?",
            "I understand. Let me help you find what you're looking for. What's your query?"
        ],
        r'\bhurry up\b': [
            "I'll search as quickly as possible. What crime information do you need?",
            "Searching now! What data should I look for?"
        ],
        r'\bjust answer\b': [
            "Absolutely. What's your question about the crime database?",
            "Got it - direct answer coming. What do you need to know?"
        ]
    }
    
    CONFUSION = {
        r'\b(confused|don\'t understand|what do you mean|huh)\b': [
            "I apologize for the confusion. Let me clarify. What part of my response was unclear?",
            "Sorry, let me explain more clearly. What would you like me to clarify?",
            "Let me simplify: What specific crime information are you looking for?"
        ],
        r'\b(i\'m lost|this doesn\'t make sense)\b': [
            "Let's start over. What crime data are you trying to find? For example: FIR number, person name, or case statistics?",
            "No problem! Tell me what you're investigating and I'll guide you step by step."
        ]
    }
    
    DISAPPOINTMENT = {
        r'\b(disappointing|expected better|not what i wanted)\b': [
            "I'm sorry I didn't provide what you needed. What crime information were you expecting?",
            "I apologize for not meeting your expectations. Could you tell me what you were hoping to find?"
        ]
    }
    
    # =========================================================================
    # 5. CLARIFICATION NEEDS (DOPAMAS Context)
    # =========================================================================
    
    CLARIFICATION_REQUESTS = {
        r'\bwhat do you mean\b': [
            "Let me clarify: I can search crime records by FIR number, person name, mobile number, crime type, location, date range, and more. What would you like to search?",
            "I mean I can help you find specific crimes, persons, drug seizures, or statistics. What information do you need?"
        ],
        r'\b(elaborate|explain better|tell me more|give me example)\b': [
            "Of course! For example, you can ask:\n• 'Find FIR 243/2022'\n• 'Show crimes in Sangareddy'\n• 'Who is Rajesh?'\n• 'Drugs seized in 2023'\n\nWhat would you like to try?",
            "Sure! Here are specific examples:\n• Search by person: 'Find Rajendra Prasad'\n• Search by location: 'Crimes in Hyderabad'\n• Search by ID: '62d29ccf37d0107b05b46f15'\n\nWhat's your query?"
        ],
        r'\b(can you be more specific|need more details)\b': [
            "Absolutely! What specific aspect would you like more details about? Crime details, person information, or statistics?",
            "Of course! Which field interests you? FIR numbers, accused details, drug seizures, or something else?"
        ]
    }
    
    # =========================================================================
    # 6. FORMAT & TONE PREFERENCES (DOPAMAS Context)
    # =========================================================================
    
    FORMAT_PREFERENCES = {
        r'\b(give me a list|show as list|list format)\b': [
            "I'll format the results as a list for you.",
            "Got it, presenting as a list."
        ],
        r'\b(keep it short|brief|quick|tldr|summary only)\b': [
            "I'll keep it concise and show only key information.",
            "Got it, showing brief summary only."
        ],
        r'\b(explain in detail|detailed|complete info|all info|everything)\b': [
            "I'll provide comprehensive details including all available fields.",
            "Showing complete information with all details."
        ],
        r'\b(simple language|easier|not technical)\b': [
            "I'll explain it in simple, everyday terms.",
            "Got it, using plain language."
        ],
        r'\b(more casual|less formal|relax)\b': [
            "Sure thing! I'll be more casual.",
            "No problem, keeping it friendly and casual!"
        ],
        r'\b(more professional|formal|official)\b': [
            "Certainly. I'll maintain a professional tone.",
            "Understood. Using formal, official language."
        ]
    }
    
    # =========================================================================
    # 7. HELP & GUIDANCE (DOPAMAS Context)
    # =========================================================================
    
    HELP_REQUESTS = {
        r'\b(help|how do i|how can i|what can you do)\b': [
            "I can help you search crime records in multiple ways:\n\n"
            "🔍 **Search by:**\n"
            "• FIR number (e.g., 'Find FIR 243/2022')\n"
            "• Person name (e.g., 'Who is Rajendra Prasad?')\n"
            "• Crime ID (e.g., 'Show crime 62d29ccf37d0107b05b46f15')\n"
            "• Mobile number (e.g., '9398922883')\n"
            "• Location (e.g., 'Crimes in Sangareddy')\n"
            "• Drug type (e.g., 'Ganja seizures')\n"
            "• Date range (e.g., 'Crimes in 2023')\n\n"
            "📊 **Get statistics:**\n"
            "• 'Count crimes by status'\n"
            "• 'Show crime trends by month'\n"
            "• 'Top districts by crime count'\n\n"
            "What would you like to search for?"
        ]
    }
    
    EXAMPLES_NEEDED = {
        r'\b(give me example|show example|for instance)\b': [
            "Here are some example queries you can try:\n\n"
            "**Person Search:**\n"
            "• 'Find Rajendra Prasad @ Sachin'\n"
            "• 'Show me details of 9398922883'\n"
            "• 'Who is from Sangareddy?'\n\n"
            "**Crime Search:**\n"
            "• 'Find FIR 243/2022'\n"
            "• 'Show crimes in last month'\n"
            "• 'Drug cases in Hyderabad'\n\n"
            "**Statistics:**\n"
            "• 'Count crimes by case status'\n"
            "• 'Show top 10 districts'\n"
            "• 'Drug seizure trends'\n\n"
            "Try one of these!"
        ]
    }
    
    # =========================================================================
    # 8. ERROR & TROUBLESHOOTING (DOPAMAS Context)
    # =========================================================================
    
    ERROR_REPORTS = {
        r'\b(error|not loading|broken|crashed)\b': [
            "I'm sorry you encountered an issue. I've logged this for investigation. Meanwhile, try:\n"
            "• Refreshing your browser\n"
            "• Asking your query in a different way\n"
            "• Checking if the data exists (e.g., verify FIR number)\n\n"
            "What were you trying to search for?"
        ],
        r'\b(can\'t find|couldn\'t find|no results)\b': [
            "If you're not finding results, try:\n"
            "• Check spelling (names, locations)\n"
            "• Use partial matches (e.g., 'Rajesh' instead of full name)\n"
            "• Try different keywords (e.g., 'narcotics' or 'drugs')\n"
            "• Search in both V1 and V2 data\n\n"
            "What are you looking for?"
        ]
    }
    
    # =========================================================================
    # 9. OUT OF SCOPE / BOUNDARY (DOPAMAS Context)
    # =========================================================================
    
    OUT_OF_SCOPE = {
        r'\b(weather|food|movie|sports|politics)\b': [
            "I'm specialized in crime investigation data only. I can help you with:\n"
            "• Crime records and FIR details\n"
            "• Accused person information\n"
            "• Drug seizure data\n"
            "• Case statistics and trends\n\n"
            "What crime-related information do you need?"
        ],
        r'\b(legal advice|lawyer|court|bail)\b': [
            "I provide crime data and statistics only, not legal advice. For legal matters, please consult a qualified attorney.\n\n"
            "I can help you with:\n"
            "• Finding case details and status\n"
            "• Accused person information\n"
            "• Crime statistics\n\n"
            "What data can I search for you?"
        ]
    }
    
    # =========================================================================
    # 10. TESTING / EDGE CASES (DOPAMAS Context)
    # =========================================================================
    
    TEST_INPUTS = {
        r'\b(test|testing|hello world)\b$': [
            "✅ System operational! DOPAMAS chatbot is ready to search crime data. Try asking:\n"
            "• 'Find crimes in Sangareddy'\n"
            "• 'Who is Rajesh?'\n"
            "• 'Show FIR 243/2022'"
        ],
        r'\b(are you (a )?bot|are you (a )?robot|are you human)\b': [
            "Yes, I'm an AI assistant powered by DOPAMAS (Drug Offence & Property Analysis & Management AI System). "
            "I can search crime records from both V1 (MongoDB) and V2 (PostgreSQL) databases. How can I help you?"
        ]
    }
    
    @classmethod
    def detect_conversation_type(cls, message: str) -> Optional[ConversationType]:
        """
        Detect the type of conversational message
        
        Args:
            message: User message
            
        Returns:
            ConversationType or None if it's a data query
        """
        # Remove trailing punctuation for better matching (but keep it for context)
        message_lower = message.lower().strip()
        # Remove trailing punctuation (?, !, .) for pattern matching
        message_clean = re.sub(r'[?!.]+$', '', message_lower).strip()
        
        # Check each pattern category (use both original and cleaned message)
        # This handles cases like "how are you?" where punctuation interferes
        for pattern in cls.GREETINGS.keys():
            if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                return ConversationType.GREETING
        
        for pattern in cls.FAREWELLS.keys():
            if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                return ConversationType.FAREWELL
        
        for pattern in cls.SMALL_TALK.keys():
            # Try both original and cleaned message
            if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                return ConversationType.SMALL_TALK
        
        for pattern in cls.ACKNOWLEDGMENTS.keys():
            # Try both original and cleaned message
            if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                return ConversationType.ACKNOWLEDGMENT
        
        for pattern in cls.GRATITUDE.keys():
            if re.search(pattern, message_lower):
                return ConversationType.GRATITUDE
        
        for pattern in cls.FRUSTRATION.keys():
            if re.search(pattern, message_lower):
                return ConversationType.FRUSTRATION
        
        for pattern in cls.CONFUSION.keys():
            if re.search(pattern, message_lower):
                return ConversationType.CONFUSION
        
        for pattern in cls.DISAPPOINTMENT.keys():
            if re.search(pattern, message_lower):
                return ConversationType.DISAPPOINTMENT
        
        for pattern in cls.CLARIFICATION_REQUESTS.keys():
            if re.search(pattern, message_lower):
                return ConversationType.CLARIFICATION
        
        for pattern in cls.FORMAT_PREFERENCES.keys():
            if re.search(pattern, message_lower):
                return ConversationType.FORMAT_PREFERENCE
        
        for pattern in cls.ERROR_REPORTS.keys():
            if re.search(pattern, message_lower):
                return ConversationType.ERROR_REPORT
        
        for pattern in cls.OUT_OF_SCOPE.keys():
            if re.search(pattern, message_lower):
                # Special handling for out-of-scope
                return None  # Will trigger redirect message
        
        for pattern in cls.TEST_INPUTS.keys():
            if re.search(pattern, message_lower):
                return ConversationType.EDGE_CASE
        
        # Check for help requests
        for pattern in cls.HELP_REQUESTS.keys():
            if re.search(pattern, message_lower):
                return ConversationType.DATA_QUERY  # Will trigger help message
        
        return None  # It's a data query, proceed normally
    
    @classmethod
    def get_response(cls, message: str, conversation_type: ConversationType) -> str:
        """
        Get appropriate conversational response
        
        Args:
            message: User message
            conversation_type: Detected conversation type
            
        Returns:
            Appropriate response string
        """
        message_lower = message.lower().strip()
        # Remove trailing punctuation for better matching
        message_clean = re.sub(r'[?!.]+$', '', message_lower).strip()
        
        # Find matching pattern and return response (try both original and cleaned)
        if conversation_type == ConversationType.GREETING:
            for pattern, responses in cls.GREETINGS.items():
                if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                    return responses[0]  # Return first response
        
        elif conversation_type == ConversationType.FAREWELL:
            for pattern, responses in cls.FAREWELLS.items():
                if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                    return responses[0]
        
        elif conversation_type == ConversationType.SMALL_TALK:
            for pattern, responses in cls.SMALL_TALK.items():
                if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                    return responses[0]
        
        elif conversation_type == ConversationType.ACKNOWLEDGMENT:
            for pattern, responses in cls.ACKNOWLEDGMENTS.items():
                if re.search(pattern, message_lower) or re.search(pattern, message_clean):
                    return responses[0]
        
        elif conversation_type == ConversationType.GRATITUDE:
            for pattern, responses in cls.GRATITUDE.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.FRUSTRATION:
            for pattern, responses in cls.FRUSTRATION.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.CONFUSION:
            for pattern, responses in cls.CONFUSION.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.DISAPPOINTMENT:
            for pattern, responses in cls.DISAPPOINTMENT.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.CLARIFICATION:
            for pattern, responses in cls.CLARIFICATION_REQUESTS.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.FORMAT_PREFERENCE:
            for pattern, responses in cls.FORMAT_PREFERENCES.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.ERROR_REPORT:
            for pattern, responses in cls.ERROR_REPORTS.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.EDGE_CASE:
            for pattern, responses in cls.TEST_INPUTS.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        elif conversation_type == ConversationType.DATA_QUERY:
            # Help request - return comprehensive help
            for pattern, responses in cls.HELP_REQUESTS.items():
                if re.search(pattern, message_lower):
                    return responses[0]
        
        # Default fallback
        return "I'm here to help you search DOPAMAS crime database. What information do you need?"
    
    @classmethod
    def should_skip_data_query(cls, conversation_type: Optional[ConversationType]) -> bool:
        """
        Determine if this is pure conversation (skip data query)
        
        Args:
            conversation_type: Detected conversation type
            
        Returns:
            True if should skip data query, False otherwise
        """
        # These types don't need database queries
        skip_types = {
            ConversationType.GREETING,
            ConversationType.FAREWELL,
            ConversationType.SMALL_TALK,
            ConversationType.ACKNOWLEDGMENT,
            ConversationType.GRATITUDE,
            ConversationType.EDGE_CASE
        }
        
        return conversation_type in skip_types
    
    @classmethod
    def extract_format_preference(cls, message: str) -> Dict[str, any]:
        """
        Extract user's format preferences from message
        
        Args:
            message: User message
            
        Returns:
            Dict with format preferences (length, style, etc.)
        """
        message_lower = message.lower()
        preferences = {
            'length': 'normal',  # 'brief', 'normal', 'detailed'
            'style': 'professional',  # 'casual', 'professional'
            'format': 'narrative'  # 'list', 'narrative', 'table'
        }
        
        # Detect length preference
        if any(kw in message_lower for kw in ['brief', 'short', 'quick', 'tldr', 'summary']):
            preferences['length'] = 'brief'
        elif any(kw in message_lower for kw in ['detail', 'complete', 'comprehensive', 'all', 'everything']):
            preferences['length'] = 'detailed'
        
        # Detect style preference
        if any(kw in message_lower for kw in ['casual', 'friendly', 'relax', 'informal']):
            preferences['style'] = 'casual'
        elif any(kw in message_lower for kw in ['professional', 'formal', 'official']):
            preferences['style'] = 'professional'
        
        # Detect format preference
        if any(kw in message_lower for kw in ['list', 'bullet', 'numbered']):
            preferences['format'] = 'list'
        elif any(kw in message_lower for kw in ['table', 'columns', 'rows']):
            preferences['format'] = 'table'
        
        return preferences



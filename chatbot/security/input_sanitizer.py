"""
Input Sanitizer
Cleans and validates user input
"""
import re
import logging
from config import Config

logger = logging.getLogger(__name__)


class InputSanitizer:
    """Sanitize user input before processing"""
    
    @staticmethod
    def sanitize_message(message: str) -> tuple[bool, str]:
        """
        Sanitize user message
        
        Args:
            message: Raw user input
        
        Returns:
            Tuple of (is_valid: bool, sanitized_message: str or error_message: str)
        """
        if not message:
            return False, "Empty message"
        
        # Remove leading/trailing whitespace
        message = message.strip()
        
        # Check length
        if len(message) > Config.MAX_INPUT_LENGTH:
            return False, f"Message too long (max {Config.MAX_INPUT_LENGTH} characters)"
        
        # ⭐ REMOVED: Minimum length check (was 3 characters)
        # Very short messages (like "f", "a", etc.) will be handled gracefully
        # by our confidence scoring system, which will ask for clarification
        # instead of showing a technical error. This provides better UX!
        # if len(message) < 3:
        #     return False, "Message too short (min 3 characters)"
        
        # Remove excessive whitespace
        message = re.sub(r'\s+', ' ', message)
        
        # Check for null bytes
        if '\x00' in message:
            return False, "Invalid characters in message"
        
        # Remove control characters except newlines and tabs
        message = ''.join(char for char in message if char.isprintable() or char in ['\n', '\t'])
        
        return True, message
    
    @staticmethod
    def sanitize_session_id(session_id: str) -> tuple[bool, str]:
        """
        Validate and sanitize session ID
        
        Args:
            session_id: Session identifier
        
        Returns:
            Tuple of (is_valid: bool, sanitized_session_id: str or error_message: str)
        """
        if not session_id:
            return False, "Empty session ID"
        
        session_id = session_id.strip()
        
        # Session ID should be alphanumeric with hyphens/underscores
        if not re.match(r'^[a-zA-Z0-9_-]{8,64}$', session_id):
            return False, "Invalid session ID format"
        
        return True, session_id




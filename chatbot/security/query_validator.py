"""
Query Validator - World-Class Security Layer
Bulletproof validation with comprehensive threat detection
"""

import re
import logging
from typing import Tuple, List, Dict, Set, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache

logger = logging.getLogger(__name__)

# ============================================================================
# Security Levels & Threat Types
# ============================================================================

class ThreatLevel(Enum):
    """Security threat levels"""
    CRITICAL = "critical"  # Data destruction, privilege escalation
    HIGH = "high"          # Data modification, injection
    MEDIUM = "medium"      # Information disclosure
    LOW = "low"            # Minor security concerns

class ThreatType(Enum):
    """Types of security threats"""
    SQL_INJECTION = "sql_injection"
    DATA_DESTRUCTION = "data_destruction"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    INFORMATION_DISCLOSURE = "information_disclosure"
    DOS_ATTACK = "dos_attack"
    UNAUTHORIZED_ACCESS = "unauthorized_access"

@dataclass
class ValidationResult:
    """Result of security validation"""
    is_safe: bool
    message: str
    threat_level: Optional[ThreatLevel] = None
    threat_type: Optional[ThreatType] = None
    blocked_patterns: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)

# ============================================================================
# Dangerous Pattern Registry
# ============================================================================

class DangerousPatterns:
    """Comprehensive registry of dangerous SQL/NoSQL patterns"""
    
    # Critical - Data destruction operations
    DESTRUCTIVE_OPERATIONS = {
        'DROP': ThreatLevel.CRITICAL,
        'TRUNCATE': ThreatLevel.CRITICAL,
        'DELETE FROM': ThreatLevel.CRITICAL,
        'ALTER TABLE': ThreatLevel.CRITICAL,
        'DROP DATABASE': ThreatLevel.CRITICAL,
        'DROP SCHEMA': ThreatLevel.CRITICAL,
    }
    
    # High - Data modification operations
    MODIFICATION_OPERATIONS = {
        'UPDATE': ThreatLevel.HIGH,
        'INSERT INTO': ThreatLevel.HIGH,
        'REPLACE INTO': ThreatLevel.HIGH,
        'MERGE': ThreatLevel.HIGH,
    }
    
    # High - System operations
    SYSTEM_OPERATIONS = {
        'EXEC': ThreatLevel.HIGH,
        'EXECUTE': ThreatLevel.HIGH,
        'CALL': ThreatLevel.HIGH,
        'SYSTEM': ThreatLevel.HIGH,
        'xp_': ThreatLevel.HIGH,  # SQL Server extended procedures
        'sp_': ThreatLevel.HIGH,  # Stored procedures
    }
    
    # High - Privilege operations
    PRIVILEGE_OPERATIONS = {
        'GRANT': ThreatLevel.HIGH,
        'REVOKE': ThreatLevel.HIGH,
        'CREATE USER': ThreatLevel.HIGH,
        'DROP USER': ThreatLevel.HIGH,
        'ALTER USER': ThreatLevel.HIGH,
    }
    
    # Medium - Schema operations
    SCHEMA_OPERATIONS = {
        'CREATE TABLE': ThreatLevel.MEDIUM,
        'CREATE INDEX': ThreatLevel.MEDIUM,
        'CREATE VIEW': ThreatLevel.MEDIUM,
        'CREATE PROCEDURE': ThreatLevel.MEDIUM,
        'CREATE FUNCTION': ThreatLevel.MEDIUM,
    }
    
    # SQL Injection patterns
    INJECTION_PATTERNS = [
        r"'\s*OR\s*'1'\s*=\s*'1",  # Classic: ' OR '1'='1
        r"'\s*OR\s*1\s*=\s*1",     # Variant: ' OR 1=1
        r"--",                      # SQL comments
        r"/\*.*?\*/",               # Block comments
        r";\s*DROP",                # Stacked queries
        r"UNION\s+SELECT",          # Union-based injection
        r"INTO\s+OUTFILE",          # File operations
        r"LOAD_FILE",               # File reading
        r"BENCHMARK\(",             # Time-based attacks
        r"SLEEP\(",                 # Time-based attacks
        r"WAITFOR\s+DELAY",         # SQL Server delays
    ]
    
    # MongoDB dangerous operations
    MONGO_DANGEROUS_STAGES = {
        '$out': ThreatLevel.CRITICAL,      # Write to collection
        '$merge': ThreatLevel.CRITICAL,    # Merge collections
        '$where': ThreatLevel.HIGH,        # JavaScript execution
        '$function': ThreatLevel.HIGH,     # Custom functions
        '$accumulator': ThreatLevel.HIGH,  # Custom accumulators
    }
    
    MONGO_DANGEROUS_OPERATORS = {
        '$eval': ThreatLevel.CRITICAL,     # JavaScript eval
        '$function': ThreatLevel.HIGH,
        '$where': ThreatLevel.HIGH,
    }

# ============================================================================
# SQL Validator
# ============================================================================

class SQLValidator:
    """Validates SQL queries for security threats"""
    
    # Compiled regex patterns for performance
    _compiled_patterns: Dict[str, re.Pattern] = {}
    
    @classmethod
    def _compile_patterns(cls):
        """Pre-compile regex patterns"""
        if not cls._compiled_patterns:
            for pattern in DangerousPatterns.INJECTION_PATTERNS:
                cls._compiled_patterns[pattern] = re.compile(pattern, re.IGNORECASE)
    
    @classmethod
    def is_safe(cls, query: str) -> Tuple[bool, str]:
        """
        Validate SQL query for security threats
        
        Args:
            query: SQL query string
        
        Returns:
            Tuple of (is_safe, message)
        """
        result = cls.validate_comprehensive(query)
        return result.is_safe, result.message
    
    @classmethod
    def validate_comprehensive(cls, query: str) -> ValidationResult:
        """
        Comprehensive validation with detailed results
        
        Args:
            query: SQL query string
        
        Returns:
            ValidationResult with full details
        """
        cls._compile_patterns()
        
        if not query or not query.strip():
            return ValidationResult(
                is_safe=False,
                message="Empty query not allowed",
                threat_level=ThreatLevel.LOW
            )
        
        query_upper = query.upper()
        query_clean = ' '.join(query.split())  # Normalize whitespace
        
        # Check 1: Destructive operations
        for operation, threat_level in DangerousPatterns.DESTRUCTIVE_OPERATIONS.items():
            if operation in query_upper:
                return ValidationResult(
                    is_safe=False,
                    message=f"Destructive operation '{operation}' not allowed",
                    threat_level=threat_level,
                    threat_type=ThreatType.DATA_DESTRUCTION,
                    blocked_patterns=[operation],
                    suggestions=[
                        "Only SELECT queries are allowed",
                        "Use application logic for data modifications"
                    ]
                )
        
        # Check 2: Modification operations
        for operation, threat_level in DangerousPatterns.MODIFICATION_OPERATIONS.items():
            if operation in query_upper:
                return ValidationResult(
                    is_safe=False,
                    message=f"Modification operation '{operation}' not allowed",
                    threat_level=threat_level,
                    threat_type=ThreatType.DATA_DESTRUCTION,
                    blocked_patterns=[operation]
                )
        
        # Check 3: System operations
        for operation, threat_level in DangerousPatterns.SYSTEM_OPERATIONS.items():
            if operation in query_upper:
                return ValidationResult(
                    is_safe=False,
                    message=f"System operation '{operation}' not allowed",
                    threat_level=threat_level,
                    threat_type=ThreatType.PRIVILEGE_ESCALATION,
                    blocked_patterns=[operation]
                )
        
        # Check 4: Privilege operations
        for operation, threat_level in DangerousPatterns.PRIVILEGE_OPERATIONS.items():
            if operation in query_upper:
                return ValidationResult(
                    is_safe=False,
                    message=f"Privilege operation '{operation}' not allowed",
                    threat_level=threat_level,
                    threat_type=ThreatType.PRIVILEGE_ESCALATION,
                    blocked_patterns=[operation]
                )
        
        # Check 5: Schema operations
        for operation, threat_level in DangerousPatterns.SCHEMA_OPERATIONS.items():
            if operation in query_upper:
                return ValidationResult(
                    is_safe=False,
                    message=f"Schema operation '{operation}' not allowed",
                    threat_level=threat_level,
                    threat_type=ThreatType.UNAUTHORIZED_ACCESS,
                    blocked_patterns=[operation]
                )
        
        # Check 6: SQL Injection patterns
        blocked = []
        for pattern_str, pattern in cls._compiled_patterns.items():
            if pattern.search(query):
                blocked.append(pattern_str)
        
        if blocked:
            return ValidationResult(
                is_safe=False,
                message="Potential SQL injection detected",
                threat_level=ThreatLevel.HIGH,
                threat_type=ThreatType.SQL_INJECTION,
                blocked_patterns=blocked,
                suggestions=[
                    "Remove SQL injection attempts",
                    "Use parameterized queries"
                ]
            )
        
        # Check 7: Must start with SELECT
        if not query_upper.strip().startswith('SELECT'):
            return ValidationResult(
                is_safe=False,
                message="Only SELECT queries are allowed",
                threat_level=ThreatLevel.HIGH,
                threat_type=ThreatType.UNAUTHORIZED_ACCESS,
                suggestions=["Query must start with SELECT"]
            )
        
        # Check 8: Query complexity (prevent DoS)
        if len(query) > 5000:
            return ValidationResult(
                is_safe=False,
                message="Query too long (max 5000 characters)",
                threat_level=ThreatLevel.MEDIUM,
                threat_type=ThreatType.DOS_ATTACK
            )
        
        # Check 9: Excessive joins (prevent DoS)
        join_count = query_upper.count('JOIN')
        if join_count > 5:
            return ValidationResult(
                is_safe=False,
                message=f"Too many JOINs ({join_count}), maximum is 5",
                threat_level=ThreatLevel.MEDIUM,
                threat_type=ThreatType.DOS_ATTACK
            )
        
        # All checks passed
        return ValidationResult(
            is_safe=True,
            message="Query is safe",
            threat_level=None
        )

# ============================================================================
# MongoDB Validator
# ============================================================================

class MongoDBValidator:
    """Validates MongoDB queries for security threats"""
    
    @classmethod
    def is_query_safe(cls, query: Dict) -> Tuple[bool, str]:
        """Validate MongoDB find query"""
        result = cls.validate_query_comprehensive(query)
        return result.is_safe, result.message
    
    @classmethod
    def is_pipeline_safe(cls, pipeline: List[Dict]) -> Tuple[bool, str]:
        """Validate MongoDB aggregation pipeline"""
        result = cls.validate_pipeline_comprehensive(pipeline)
        return result.is_safe, result.message
    
    @classmethod
    def validate_query_comprehensive(cls, query: Dict) -> ValidationResult:
        """Comprehensive validation of MongoDB query"""
        if not isinstance(query, dict):
            return ValidationResult(
                is_safe=False,
                message="Query must be a dictionary",
                threat_level=ThreatLevel.LOW
            )
        
        # Check for dangerous operators
        blocked = cls._check_dangerous_operators(query)
        if blocked:
            return ValidationResult(
                is_safe=False,
                message=f"Dangerous operators detected: {', '.join(blocked)}",
                threat_level=ThreatLevel.HIGH,
                threat_type=ThreatType.PRIVILEGE_ESCALATION,
                blocked_patterns=blocked,
                suggestions=["Remove JavaScript execution operators"]
            )
        
        # Check query complexity
        if cls._calculate_depth(query) > 10:
            return ValidationResult(
                is_safe=False,
                message="Query too complex (max depth: 10)",
                threat_level=ThreatLevel.MEDIUM,
                threat_type=ThreatType.DOS_ATTACK
            )
        
        return ValidationResult(is_safe=True, message="Query is safe")
    
    @classmethod
    def validate_pipeline_comprehensive(cls, pipeline: List[Dict]) -> ValidationResult:
        """Comprehensive validation of aggregation pipeline"""
        if not isinstance(pipeline, list):
            return ValidationResult(
                is_safe=False,
                message="Pipeline must be a list",
                threat_level=ThreatLevel.LOW
            )
        
        if len(pipeline) > 20:
            return ValidationResult(
                is_safe=False,
                message="Pipeline too long (max 20 stages)",
                threat_level=ThreatLevel.MEDIUM,
                threat_type=ThreatType.DOS_ATTACK
            )
        
        # Check each stage
        for i, stage in enumerate(pipeline):
            if not isinstance(stage, dict):
                return ValidationResult(
                    is_safe=False,
                    message=f"Stage {i} must be a dictionary",
                    threat_level=ThreatLevel.LOW
                )
            
            # Check for dangerous stages
            for stage_name, threat_level in DangerousPatterns.MONGO_DANGEROUS_STAGES.items():
                if stage_name in stage:
                    return ValidationResult(
                        is_safe=False,
                        message=f"Dangerous stage '{stage_name}' not allowed",
                        threat_level=threat_level,
                        threat_type=ThreatType.DATA_DESTRUCTION,
                        blocked_patterns=[stage_name],
                        suggestions=["Only read operations are allowed"]
                    )
            
            # Check operators in stage
            blocked = cls._check_dangerous_operators(stage)
            if blocked:
                return ValidationResult(
                    is_safe=False,
                    message=f"Dangerous operators in stage {i}: {', '.join(blocked)}",
                    threat_level=ThreatLevel.HIGH,
                    threat_type=ThreatType.PRIVILEGE_ESCALATION,
                    blocked_patterns=blocked
                )
        
        return ValidationResult(is_safe=True, message="Pipeline is safe")
    
    @classmethod
    def _check_dangerous_operators(cls, obj: Any, depth: int = 0) -> List[str]:
        """Recursively check for dangerous operators"""
        if depth > 10:  # Prevent infinite recursion
            return []
        
        blocked = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Check if key is dangerous operator
                if key in DangerousPatterns.MONGO_DANGEROUS_OPERATORS:
                    blocked.append(key)
                
                # Recursively check values
                blocked.extend(cls._check_dangerous_operators(value, depth + 1))
        
        elif isinstance(obj, list):
            for item in obj:
                blocked.extend(cls._check_dangerous_operators(item, depth + 1))
        
        return blocked
    
    @classmethod
    def _calculate_depth(cls, obj: Any, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth"""
        if not isinstance(obj, (dict, list)):
            return current_depth
        
        max_depth = current_depth
        
        if isinstance(obj, dict):
            for value in obj.values():
                depth = cls._calculate_depth(value, current_depth + 1)
                max_depth = max(max_depth, depth)
        elif isinstance(obj, list):
            for item in obj:
                depth = cls._calculate_depth(item, current_depth + 1)
                max_depth = max(max_depth, depth)
        
        return max_depth

# ============================================================================
# Error Sanitizer
# ============================================================================

class ErrorSanitizer:
    """Sanitizes error messages to prevent information disclosure"""
    
    # Patterns to remove from error messages
    SENSITIVE_PATTERNS = [
        (r'Table\s+["\']?(\w+)["\']?', 'Table [REDACTED]'),
        (r'Column\s+["\']?(\w+)["\']?', 'Column [REDACTED]'),
        (r'Database\s+["\']?(\w+)["\']?', 'Database [REDACTED]'),
        (r'User\s+["\']?(\w+)["\']?', 'User [REDACTED]'),
        (r'Host\s+["\']?[\w\.\-]+["\']?', 'Host [REDACTED]'),
        (r'Password', '[REDACTED]'),
        (r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', '[IP REDACTED]'),
        (r':\d{4,5}', ':[PORT]'),  # Port numbers
    ]
    
    @classmethod
    def sanitize(cls, error_message: str) -> str:
        """
        Sanitize error message to remove sensitive information
        
        Args:
            error_message: Original error message
        
        Returns:
            Sanitized error message
        """
        if not error_message:
            return "An error occurred"
        
        sanitized = str(error_message)
        
        # Apply all sanitization patterns
        for pattern, replacement in cls.SENSITIVE_PATTERNS:
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
        
        # Truncate if too long
        if len(sanitized) > 200:
            sanitized = sanitized[:200] + "... [truncated]"
        
        return sanitized

# ============================================================================
# Main Query Validator
# ============================================================================

class QueryValidator:
    """
    World-class query validator with comprehensive security
    """
    
    # Class-level instances
    sql_validator = SQLValidator()
    mongo_validator = MongoDBValidator()
    error_sanitizer = ErrorSanitizer()
    
    @classmethod
    def is_safe(cls, query: str) -> Tuple[bool, str]:
        """
        Validate SQL query (backward compatible with existing code)
        
        Args:
            query: SQL query string
        
        Returns:
            Tuple of (is_safe, message)
        """
        return cls.sql_validator.is_safe(query)
    
    @classmethod
    def is_sql_safe(cls, query: str) -> Tuple[bool, str]:
        """Validate SQL query"""
        return cls.sql_validator.is_safe(query)
    
    @classmethod
    def is_mongo_query_safe(cls, query: Dict) -> Tuple[bool, str]:
        """Validate MongoDB find query"""
        return cls.mongo_validator.is_query_safe(query)
    
    @classmethod
    def is_mongo_pipeline_safe(cls, pipeline: List[Dict]) -> Tuple[bool, str]:
        """Validate MongoDB pipeline"""
        return cls.mongo_validator.is_pipeline_safe(pipeline)
    
    @classmethod
    def sanitize_error_message(cls, error: str) -> str:
        """Sanitize error message"""
        return cls.error_sanitizer.sanitize(error)
    
    @classmethod
    def validate_sql_detailed(cls, query: str) -> ValidationResult:
        """Get detailed SQL validation results"""
        return cls.sql_validator.validate_comprehensive(query)
    
    @classmethod
    def validate_mongo_detailed(cls, query: Dict) -> ValidationResult:
        """Get detailed MongoDB validation results"""
        return cls.mongo_validator.validate_query_comprehensive(query)
    
    @classmethod
    def validate_mongo_pipeline_detailed(cls, pipeline: List[Dict]) -> ValidationResult:
        """Get detailed MongoDB pipeline validation results"""
        return cls.mongo_validator.validate_pipeline_comprehensive(pipeline)


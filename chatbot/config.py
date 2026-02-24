"""
Configuration Management
Loads all settings from .env file
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Application Configuration"""
    
    # Flask
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    PORT = int(os.getenv('FLASK_PORT', 5000))
    
    # PostgreSQL
    POSTGRES_CONFIG = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'database': os.getenv('POSTGRES_DB', 'postgres'),
        'user': os.getenv('POSTGRES_USER', 'readonly_user'),
        'password': os.getenv('POSTGRES_PASSWORD', ''),
    }
    
    # MongoDB
    MONGO_CONFIG = {
        'host': os.getenv('MONGO_HOST', 'localhost'),
        'port': int(os.getenv('MONGO_PORT', 27017)),
        'database': os.getenv('MONGO_DB', 'test'),
        'username': os.getenv('MONGO_USER', ''),
        'password': os.getenv('MONGO_PASSWORD', ''),
        'authSource': os.getenv('MONGO_AUTH_SOURCE', 'admin'),
    }
    
    # Redis
    REDIS_CONFIG = {
        'host': os.getenv('REDIS_HOST', 'localhost'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
        'password': os.getenv('REDIS_PASSWORD', None),
        'decode_responses': True,
    }
    
    # LLM Configuration
    LLM_CONFIG = {
        'provider': os.getenv('LLM_PROVIDER', 'ollama'),  # 'ollama', 'openai', or 'anthropic'
        'api_url': os.getenv('LLM_API_URL', 'http://localhost:11434'),
        'api_key': os.getenv('LLM_API_KEY', ''),  # For OpenAI/Anthropic
        'model': os.getenv('LLM_MODEL_SQL', 'deepseek-coder-v2:16b'),  # OPTIMIZED for SQL generation — routed via LLM_MODEL_SQL
        'temperature': float(os.getenv('LLM_TEMPERATURE', 0.0)),  # 0.0 for deterministic SQL
        'max_tokens': int(os.getenv('LLM_MAX_TOKENS', 1000)),  # 1000 for complex multi-table queries
        'timeout': int(os.getenv('LLM_TIMEOUT_SECONDS', 120)),  # 120s for complex queries
    }
    
    # Security
    RATE_LIMIT = int(os.getenv('RATE_LIMIT_PER_MINUTE', 30))
    MAX_INPUT_LENGTH = int(os.getenv('MAX_INPUT_LENGTH', 1000))
    MAX_QUERY_ROWS = int(os.getenv('MAX_QUERY_ROWS', 100))  # Reduced from 1000 to 100 for performance
    QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT_SECONDS', 30))
    
    # Cache TTL
    SCHEMA_CACHE_TTL = int(os.getenv('SCHEMA_CACHE_TTL', 7200))
    QUERY_CACHE_TTL = int(os.getenv('QUERY_CACHE_TTL', 1800))
    HISTORY_CACHE_TTL = int(os.getenv('HISTORY_CACHE_TTL', 3600))
    NARRATIVE_CACHE_TTL = int(os.getenv('NARRATIVE_CACHE_TTL', 3600))  # 1 hour for narratives
    
    # Agent Configuration
    ENABLE_NARRATIVE_FORMATTING = os.getenv('ENABLE_NARRATIVE_FORMATTING', 'true').lower() == 'true'
    USE_SPACY_NER = os.getenv('USE_SPACY_NER', 'true').lower() == 'true'
    
    # Session
    SESSION_LIFETIME_HOURS = int(os.getenv('SESSION_LIFETIME_HOURS', 24))




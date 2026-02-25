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
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
    DEBUG = os.getenv('FLASK_DEBUG') == 'true'
    PORT = int(os.getenv('FLASK_PORT'))
    
    # PostgreSQL
    POSTGRES_CONFIG = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': int(os.getenv('POSTGRES_PORT')),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
    }
    
    # MongoDB
    MONGO_CONFIG = {
        'host': os.getenv('MONGO_HOST'),
        'port': int(os.getenv('MONGO_PORT')),
        'database': os.getenv('MONGO_DB'),
        'username': os.getenv('MONGO_USER'),
        'password': os.getenv('MONGO_PASSWORD'),
        'authSource': os.getenv('MONGO_AUTH_SOURCE'),
    }
    
    # Redis
    REDIS_CONFIG = {
        'host': os.getenv('REDIS_HOST'),
        'port': int(os.getenv('REDIS_PORT')),
        'db': int(os.getenv('REDIS_DB')),
        'password': os.getenv('REDIS_PASSWORD'),
        'decode_responses': True,
    }
    
    # LLM Configuration
    LLM_CONFIG = {
        'provider': os.getenv('LLM_PROVIDER'),  # 'ollama', 'openai', or 'anthropic'
        'api_url': os.getenv('LLM_API_URL'),
        'api_key': os.getenv('LLM_API_KEY'),  # For OpenAI/Anthropic
        'model': os.getenv('LLM_MODEL_SQL'),  # OPTIMIZED for SQL generation — routed via LLM_MODEL_SQL
        'temperature': float(os.getenv('LLM_TEMPERATURE')),  # 0.0 for deterministic SQL
        'max_tokens': int(os.getenv('LLM_MAX_TOKENS')),  # 1000 for complex multi-table queries
        'timeout': int(os.getenv('LLM_TIMEOUT_SECONDS')),  # 120s for complex queries
    }
    
    # Security
    RATE_LIMIT = int(os.getenv('RATE_LIMIT_PER_MINUTE'))
    MAX_INPUT_LENGTH = int(os.getenv('MAX_INPUT_LENGTH'))
    MAX_QUERY_ROWS = int(os.getenv('MAX_QUERY_ROWS'))  # Reduced from 1000 to 100 for performance
    QUERY_TIMEOUT = int(os.getenv('QUERY_TIMEOUT_SECONDS'))
    
    # Cache TTL
    SCHEMA_CACHE_TTL = int(os.getenv('SCHEMA_CACHE_TTL'))
    QUERY_CACHE_TTL = int(os.getenv('QUERY_CACHE_TTL'))
    HISTORY_CACHE_TTL = int(os.getenv('HISTORY_CACHE_TTL'))
    NARRATIVE_CACHE_TTL = int(os.getenv('NARRATIVE_CACHE_TTL'))  # 1 hour for narratives
    
    # Agent Configuration
    ENABLE_NARRATIVE_FORMATTING = os.getenv('ENABLE_NARRATIVE_FORMATTING') == 'true'
    USE_SPACY_NER = os.getenv('USE_SPACY_NER') == 'true'
    
    # Session
    SESSION_LIFETIME_HOURS = int(os.getenv('SESSION_LIFETIME_HOURS'))




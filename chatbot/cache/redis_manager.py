"""
Redis Cache Manager
Handles all caching operations for schema, queries, and sessions
"""
import redis
import json
import hashlib
import logging
from typing import Any, Optional
from config import Config

logger = logging.getLogger(__name__)


class RedisManager:
    """Manage Redis caching operations"""
    
    def __init__(self):
        self.client = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize Redis connection"""
        try:
            redis_config = Config.REDIS_CONFIG
            self.client = redis.Redis(
                host=redis_config['host'],
                port=redis_config['port'],
                db=redis_config['db'],
                password=redis_config['password'] if redis_config['password'] else None,
                decode_responses=redis_config['decode_responses'],
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.client.ping()
            logger.info("Redis connection initialized")
            
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.client = None
        except Exception as e:
            logger.error(f"Redis initialization error: {e}")
            self.client = None
    
    def is_available(self) -> bool:
        """Check if Redis is available"""
        if not self.client:
            return False
        try:
            return self.client.ping()
        except:
            return False
    
    def set_value(self, key: str, value: Any, ttl: int = None) -> bool:
        """
        Set a value in cache with optional TTL
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds
        
        Returns:
            bool: Success status
        """
        if not self.is_available():
            logger.warning("Redis not available, skipping cache set")
            return False
        
        try:
            serialized_value = json.dumps(value, default=str)
            
            if ttl:
                self.client.setex(key, ttl, serialized_value)
            else:
                self.client.set(key, serialized_value)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to set cache key {key}: {e}")
            return False
    
    def get_value(self, key: str) -> Optional[Any]:
        """
        Get a value from cache
        
        Args:
            key: Cache key
        
        Returns:
            Cached value or None if not found
        """
        if not self.is_available():
            return None
        
        try:
            value = self.client.get(key)
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get cache key {key}: {e}")
            return None
    
    def delete_key(self, key: str) -> bool:
        """Delete a key from cache"""
        if not self.is_available():
            return False
        
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete cache key {key}: {e}")
            return False
    
    def cache_schema(self, schema: dict) -> bool:
        """
        Cache database schema with version tracking
        Auto-invalidates when schema changes!
        """
        # Calculate schema version based on table names
        schema_version = self._calculate_schema_version(schema)
        
        # Store schema with version
        versioned_schema = {
            'version': schema_version,
            'schema': schema
        }
        
        logger.info(f"Caching schema with version: {schema_version}")
        return self.set_value('schema_info', versioned_schema, Config.SCHEMA_CACHE_TTL)
    
    def get_cached_schema(self) -> Optional[dict]:
        """
        Get cached database schema
        Auto-invalidates if schema version changed!
        """
        cached_data = self.get_value('schema_info')
        
        if not cached_data:
            return None
        
        # Check if this is old format (no version)
        if 'version' not in cached_data:
            logger.warning("Old cache format detected - invalidating")
            self.delete_key('schema_info')
            return None
        
        cached_version = cached_data.get('version')
        cached_schema = cached_data.get('schema')
        
        # Calculate current schema version to compare
        current_version = self._calculate_schema_version(cached_schema)
        
        if cached_version != current_version:
            logger.warning(f"Schema version mismatch (cached: {cached_version}, current: {current_version}) - invalidating cache")
            self.delete_key('schema_info')
            return None
        
        logger.info(f"Using cached schema (version: {cached_version})")
        return cached_schema
    
    def _calculate_schema_version(self, schema: dict) -> str:
        """
        Calculate schema version hash based on table/collection names
        Changes when tables are added/removed
        """
        # Get sorted list of all table/collection names
        pg_tables = sorted(schema.get('postgresql', {}).keys())
        mongo_collections = sorted(schema.get('mongodb', {}).keys())
        
        # Create version string
        version_string = f"pg:{','.join(pg_tables)}|mongo:{','.join(mongo_collections)}"
        
        # Hash it
        version_hash = hashlib.md5(version_string.encode()).hexdigest()[:8]
        
        return version_hash
    
    def cache_query_result(self, query: str, result: Any) -> bool:
        """
        Cache query result with hash-based key
        
        Args:
            query: Query string
            result: Query result
        
        Returns:
            bool: Success status
        """
        # Generate cache key from query hash
        query_hash = hashlib.md5(query.encode()).hexdigest()
        cache_key = f'query_cache:{query_hash}'
        
        return self.set_value(cache_key, result, Config.QUERY_CACHE_TTL)
    
    def get_cached_query_result(self, query: str) -> Optional[Any]:
        """
        Get cached query result
        
        Args:
            query: Query string
        
        Returns:
            Cached result or None
        """
        query_hash = hashlib.md5(query.encode()).hexdigest()
        cache_key = f'query_cache:{query_hash}'
        
        return self.get_value(cache_key)
    
    def add_to_history(self, session_id: str, user_message: str, assistant_response: str) -> bool:
        """
        Add conversation to history
        
        Args:
            session_id: Session identifier
            user_message: User's message
            assistant_response: Assistant's response
        
        Returns:
            bool: Success status
        """
        if not self.is_available():
            return False
        
        try:
            history_key = f'history:{session_id}'
            
            # Get current history
            history = self.get_value(history_key) or []
            
            # Add new exchange
            from datetime import datetime
            history.append({
                'user': user_message,
                'assistant': assistant_response,
                'timestamp': datetime.now().isoformat()
            })
            
            # Keep only last 10 messages
            history = history[-10:]
            
            # Save back with TTL
            return self.set_value(history_key, history, Config.HISTORY_CACHE_TTL)
            
        except Exception as e:
            logger.error(f"Failed to add to history: {e}")
            return False
    
    def get_history(self, session_id: str) -> list:
        """
        Get conversation history for a session
        
        Args:
            session_id: Session identifier
        
        Returns:
            List of conversation exchanges
        """
        history_key = f'history:{session_id}'
        return self.get_value(history_key) or []
    
    def clear_history(self, session_id: str) -> bool:
        """Clear conversation history for a session"""
        history_key = f'history:{session_id}'
        return self.delete_key(history_key)
    
    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()
            logger.info("Redis connection closed")




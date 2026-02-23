"""
Flask API Routes
Defines all HTTP endpoints for the chatbot
"""
from flask import Blueprint, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
import uuid
from security.input_sanitizer import InputSanitizer
from config import Config

logger = logging.getLogger(__name__)

# Create blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')

# Rate limiter will be initialized in app.py
limiter = Limiter(key_func=get_remote_address)


def init_routes(agent, cache_manager, postgres_executor, mongo_executor):
    """
    Initialize routes with dependencies
    
    Args:
        agent: DatabaseQueryAgent instance
        cache_manager: RedisManager instance
        postgres_executor: PostgreSQLExecutor instance
        mongo_executor: MongoDBExecutor instance
    """
    
    @api_bp.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        health_status = {
            'status': 'healthy',
            'services': {
                'postgresql': postgres_executor.test_connection(),
                'mongodb': mongo_executor.test_connection(),
                'redis': cache_manager.is_available()
            }
        }
        
        # Overall health
        all_healthy = all(health_status['services'].values())
        health_status['status'] = 'healthy' if all_healthy else 'degraded'
        
        status_code = 200 if all_healthy else 503
        
        return jsonify(health_status), status_code
    
    @api_bp.route('/chat', methods=['POST'])
    @limiter.limit(f"{Config.RATE_LIMIT} per minute")
    def chat():
        """
        Main chat endpoint
        
        Request JSON:
            {
                "message": "Show me all users",
                "session_id": "optional-session-id"
            }
        
        Response JSON:
            {
                "success": true,
                "response": "Found 10 results...",
                "queries": {...},
                "session_id": "session-id"
            }
        """
        try:
            # Get request data
            data = request.get_json()
            
            if not data:
                return jsonify({
                    'success': False,
                    'error': 'No JSON data provided'
                }), 400
            
            # Extract and validate message
            message = data.get('message', '')
            is_valid, result = InputSanitizer.sanitize_message(message)
            
            if not is_valid:
                return jsonify({
                    'success': False,
                    'error': result
                }), 400
            
            sanitized_message = result
            
            # Get or create session ID
            session_id = data.get('session_id')
            if session_id:
                is_valid, result = InputSanitizer.sanitize_session_id(session_id)
                if not is_valid:
                    session_id = str(uuid.uuid4())
            else:
                session_id = str(uuid.uuid4())
            
            logger.info(f"Chat request - Session: {session_id}, Message: {sanitized_message[:50]}...")
            
            # Process message through agent
            response = agent.process_message(sanitized_message, session_id)
            
            # Add to conversation history
            if response['success']:
                cache_manager.add_to_history(
                    session_id,
                    sanitized_message,
                    response['response']
                )
            
            # Add session ID to response
            response['session_id'] = session_id
            
            return jsonify(response), 200
            
        except Exception as e:
            logger.error(f"Chat endpoint error: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500
    
    @api_bp.route('/chat/history/<session_id>', methods=['GET'])
    @limiter.limit(f"{Config.RATE_LIMIT} per minute")
    def get_history(session_id):
        """
        Get conversation history for a session
        
        Response JSON:
            {
                "session_id": "session-id",
                "history": [
                    {
                        "user": "message",
                        "assistant": "response",
                        "timestamp": "2025-11-06T10:30:00Z"
                    }
                ]
            }
        """
        try:
            # Validate session ID
            is_valid, result = InputSanitizer.sanitize_session_id(session_id)
            
            if not is_valid:
                return jsonify({
                    'success': False,
                    'error': 'Invalid session ID'
                }), 400
            
            # Get history from cache
            history = cache_manager.get_history(session_id)
            
            return jsonify({
                'success': True,
                'session_id': session_id,
                'history': history
            }), 200
            
        except Exception as e:
            logger.error(f"History endpoint error: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500
    
    @api_bp.route('/chat/history/<session_id>', methods=['DELETE'])
    @limiter.limit(f"{Config.RATE_LIMIT} per minute")
    def clear_history(session_id):
        """Clear conversation history for a session"""
        try:
            is_valid, result = InputSanitizer.sanitize_session_id(session_id)
            
            if not is_valid:
                return jsonify({
                    'success': False,
                    'error': 'Invalid session ID'
                }), 400
            
            cache_manager.clear_history(session_id)
            
            return jsonify({
                'success': True,
                'message': 'History cleared'
            }), 200
            
        except Exception as e:
            logger.error(f"Clear history endpoint error: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500
    
    @api_bp.route('/schema', methods=['GET'])
    @limiter.limit(f"{Config.RATE_LIMIT} per minute")
    def get_schema():
        """
        Get database schemas
        
        Response JSON:
            {
                "postgresql": {...},
                "mongodb": {...}
            }
        """
        try:
            # Try cache first
            schema = cache_manager.get_cached_schema()
            
            if not schema:
                # Fetch fresh schema
                from database.schema_manager import SchemaManager
                schema_manager = SchemaManager(postgres_executor, mongo_executor)
                schema = schema_manager.get_combined_schema()
                
                # Cache it
                cache_manager.cache_schema(schema)
            
            return jsonify({
                'success': True,
                'schema': schema
            }), 200
            
        except Exception as e:
            logger.error(f"Schema endpoint error: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': 'Internal server error'
            }), 500
    
    @api_bp.route('/query/validate', methods=['POST'])
    @limiter.limit(f"{Config.RATE_LIMIT} per minute")
    def validate_query():
        """
        Test query validation
        
        Request JSON:
            {
                "query": "SELECT * FROM users",
                "type": "sql" or "mongodb"
            }
        
        Response JSON:
            {
                "valid": true,
                "message": "Query is safe"
            }
        """
        try:
            from security.query_validator import QueryValidator
            
            data = request.get_json()
            
            if not data or 'query' not in data:
                return jsonify({
                    'valid': False,
                    'message': 'No query provided'
                }), 400
            
            query = data['query']
            query_type = data.get('type', 'sql').lower()
            
            if query_type == 'sql':
                is_safe, message = QueryValidator.is_sql_safe(query)
            else:
                # Assume MongoDB query as dict
                import json
                try:
                    query_dict = json.loads(query) if isinstance(query, str) else query
                    is_safe, message = QueryValidator.is_mongo_query_safe(query_dict)
                except:
                    is_safe, message = False, "Invalid MongoDB query format"
            
            return jsonify({
                'valid': is_safe,
                'message': message
            }), 200
            
        except Exception as e:
            logger.error(f"Validate endpoint error: {e}", exc_info=True)
            return jsonify({
                'valid': False,
                'message': 'Validation error'
            }), 500
    
    return api_bp




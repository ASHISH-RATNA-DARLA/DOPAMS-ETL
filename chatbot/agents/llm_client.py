"""
Local LLM Client
Connects to Qwen2.5-Coder running locally via Ollama or similar
"""
import requests
import logging
from typing import Dict, Any, Optional
from config import Config

logger = logging.getLogger(__name__)


class LocalLLMClient:
    """Client for local Qwen2.5-Coder LLM"""
    
    def __init__(self):
        self.api_url = Config.LLM_CONFIG['api_url']
        self.model = Config.LLM_CONFIG['model']
        self.temperature = Config.LLM_CONFIG['temperature']
        self.max_tokens = Config.LLM_CONFIG['max_tokens']
        self.timeout = Config.LLM_CONFIG['timeout']
    
    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """
        Generate text using local LLM
        
        Args:
            prompt: User prompt
            system_prompt: Optional system prompt for context
        
        Returns:
            Generated text or None if failed
        """
        try:
            # Ollama API format
            endpoint = f"{self.api_url}/api/generate"
            
            # Combine system and user prompts
            full_prompt = prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"
            
            payload = {
                "model": self.model,
                "prompt": full_prompt,
                "stream": False,
                "options": {
                    "temperature": self.temperature,
                    "num_predict": self.max_tokens
                }
            }
            
            logger.info(f"Sending request to LLM: {self.model}")
            
            response = requests.post(
                endpoint,
                json=payload,
                timeout=self.timeout  # Configurable timeout from .env
            )
            
            response.raise_for_status()
            
            result = response.json()
            generated_text = result.get('response', '').strip()
            
            logger.info(f"LLM response received ({len(generated_text)} chars)")
            
            return generated_text
            
        except requests.exceptions.Timeout:
            logger.error("LLM request timed out")
            return None
            
        except requests.exceptions.ConnectionError:
            logger.error(f"Could not connect to LLM at {self.api_url}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"LLM request failed: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error in LLM generation: {e}")
            return None
    
    def generate_sql(self, user_message: str, schema: str) -> Optional[str]:
        """
        Generate SQL query from natural language
        
        Args:
            user_message: User's natural language query
            schema: Database schema information
        
        Returns:
            SQL query string or None
        """
        system_prompt = """You are an expert SQL query generator. 
Given a database schema and a user's natural language question, generate a valid SQL SELECT query.

RULES:
1. Generate ONLY the SQL query, no explanations
2. Use only SELECT statements
3. Use proper table and column names from the schema
4. Add appropriate WHERE, ORDER BY, LIMIT clauses as needed
5. If the query is ambiguous, make reasonable assumptions
6. Do not include semicolons at the end
7. Use standard SQL syntax compatible with PostgreSQL"""
        
        prompt = f"""Database Schema:
{schema}

User Question: {user_message}

Generate the SQL query:"""
        
        return self.generate(prompt, system_prompt)
    
    def generate_mongodb_query(self, user_message: str, schema: str) -> Optional[str]:
        """
        Generate MongoDB query from natural language
        
        Args:
            user_message: User's natural language query
            schema: Database schema information
        
        Returns:
            MongoDB query as JSON string or None
        """
        system_prompt = """You are an expert MongoDB query generator.
Given a MongoDB schema and a user's natural language question, generate a valid MongoDB query.

RULES:
1. Return ONLY valid JSON representing the query
2. Use find() queries or aggregation pipelines as appropriate
3. Use proper collection and field names from the schema
4. For simple queries, return: {"collection": "name", "query": {...}, "projection": {...}}
5. For aggregations, return: {"collection": "name", "pipeline": [...]}
6. Do not include explanations, only JSON
7. Ensure the JSON is properly formatted"""
        
        prompt = f"""MongoDB Schema:
{schema}

User Question: {user_message}

Generate the MongoDB query (JSON only):"""
        
        return self.generate(prompt, system_prompt)
    
    def detect_intent(self, user_message: str) -> str:
        """
        Detect user intent from message
        
        Args:
            user_message: User's message
        
        Returns:
            Intent: 'query', 'aggregation', 'clarification', or 'general'
        """
        message_lower = user_message.lower()
        
        # Simple rule-based intent detection
        query_keywords = ['show', 'get', 'find', 'list', 'select', 'fetch', 'retrieve', 'display']
        aggregation_keywords = ['count', 'sum', 'average', 'total', 'group', 'aggregate', 'max', 'min']
        
        if any(keyword in message_lower for keyword in aggregation_keywords):
            return 'aggregation'
        
        if any(keyword in message_lower for keyword in query_keywords):
            return 'query'
        
        if '?' in user_message or any(word in message_lower for word in ['what', 'how', 'which', 'when']):
            return 'query'
        
        return 'general'
    
    def test_connection(self) -> bool:
        """Test if LLM is accessible"""
        try:
            response = requests.get(f"{self.api_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False




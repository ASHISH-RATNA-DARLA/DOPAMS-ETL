import os
import json
import logging
from typing import Dict, Any, Optional
from functools import lru_cache

from dotenv import load_dotenv

# Load central environment configurations
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

# Fallback defaults if env vars are missing
DEFAULT_OLLAMA_HOST = "http://localhost:11434"

class LLMService:
    """Unified Service for LLM API Generation"""
    
    def __init__(self, model: str, temperature: float = 0.0, max_tokens: int = 1000, context_window: int = 4096, stream: bool = False):
        self.api_url = os.getenv("OLLAMA_HOST", DEFAULT_OLLAMA_HOST)
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.context_window = context_window
        self.stream = stream
        self._langchain_model_instance = None

    def get_langchain_model(self):
        """Returns a Langchain ChatOllama instance for structured abstraction tasks
        Caches the model instance internally to prevent multiple instantiations per service."""
        
        if self._langchain_model_instance is not None:
            return self._langchain_model_instance
            
        from langchain_ollama import ChatOllama
        
        # Ensure base URL is correctly formatted for Langchain (remove trailing /api if present in some configs)
        base_url = self.api_url
        if base_url.endswith("/api"):
            base_url = base_url.replace("/api", "")
            
        self._langchain_model_instance = ChatOllama(
            base_url=base_url,
            model=self.model,
            temperature=self.temperature,
            num_ctx=self.context_window,
            # Use max_tokens internally if strictly required by underlying Langchain versions
        )
        return self._langchain_model_instance

    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """Direct HTTP generation primarily used for Chatbot SQL and legacy routing"""
        import requests
        
        endpoint = f"{self.api_url}/api/generate"
        if not self.api_url.endswith("/api") and not endpoint.endswith("/api/generate"):
             # Normalise ollama urls
             endpoint = f"{self.api_url.rstrip('/')}/api/generate"
        
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"
            
        payload = {
            "model": self.model,
            "prompt": full_prompt,
            "stream": self.stream,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
                "num_ctx": self.context_window
            }
        }
        
        logger.info(f"Sending request to LLM: {self.model} with context_window {self.context_window}")
        
        try:
            # Explicit timeout to prevent silent hangs
            response = requests.post(endpoint, json=payload, timeout=120)
            response.raise_for_status()
            
            result = response.json()
            return result.get('response', '').strip()
            
        except requests.exceptions.Timeout:
            logger.error("LLM request timed out")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"Could not connect to LLM at {endpoint}")
            return None
        except Exception as e:
            logger.error(f"LLM request failed: {e}")
            return None

@lru_cache(maxsize=10)
def get_llm(task_type: str) -> LLMService:
    """
    Factory Function to route the correct LLM model, temperature, and context based on task.
    """
    task_type = task_type.lower()
    
    if task_type == 'extraction':
        model = os.getenv("LLM_MODEL_EXTRACTION", "deepseek-coder-v2:16b")
        return LLMService(
            model=model,
            temperature=0.0,          # Deterministic JSON
            max_tokens=4096,          # Allow large JSON responses
            context_window=16384,     # Crucial for preventing FIR truncation
            stream=False
        )
        
    elif task_type == 'sql':
        model = os.getenv("LLM_MODEL_SQL", "deepseek-coder-v2:16b")
        return LLMService(
            model=model,
            temperature=0.0,
            max_tokens=1500,
            context_window=4096,
            stream=False              # Disabled streaming as required by chatbot architecture
        )
        
    elif task_type == 'classification':
        model = os.getenv("LLM_MODEL_CLASSIFICATION", "llama3.1:8b")
        return LLMService(
            model=model,
            temperature=0.1,          # Slight variability is okay, mostly deterministic
            max_tokens=512,
            context_window=2048,      # Small context window for speed
            stream=False
        )
        
    elif task_type == 'reasoning':
        model = os.getenv("LLM_MODEL_REASONING", "falcon3:10b")
        return LLMService(
            model=model,
            temperature=0.2,          # Requires some reasoning variance
            max_tokens=1000,
            context_window=4096,
            stream=False
        )
        
    else:
        logger.warning(f"Unknown task_type '{task_type}', defaulting to extraction parameters.")
        model = os.getenv("LLM_MODEL_EXTRACTION", "deepseek-coder-v2:16b")
        return LLMService(model=model)

# --- Retry Loop Wrapper for Extraction ---

def invoke_extraction_with_retry(chain, input_data: dict, max_retries: int = 1) -> dict:
    """
    Executes a LangChain extraction chain. If LangChain's JsonOutputParser 
    throws an error (e.g., truncated JSON, missing brackets), it catches it 
    and automatically retries the prompt with a strong correction instruction.
    """
    from langchain_core.exceptions import OutputParserException
    
    retries = 0
    last_error = None
    
    # Run once normally
    try:
        return chain.invoke(input_data)
    except OutputParserException as e:
        logger.warning(f"JSON Parsing failed on first attempt: {e}")
        last_error = e
        retries += 1
        
    # Retry loop
    while retries <= max_retries:
        logger.info(f"Retry {retries}/{max_retries} for JSON Extraction")
        
        # We assume input_data has a 'text' or similar prompt input.
        # We append a strong system override to force correct JSON formatting.
        correction_instruction = (
            "\n\n[SYSTEM OVERRIDE: Your previous response was INVALID JSON. "
            "You MUST fix the JSON formatting to exactly match the requested schema. "
            "Ensure all brackets are closed and keys are properly quoted.]"
        )
        
        # Try to intelligently inject the correction instruction into the main payload text
        retry_data = input_data.copy()
        for key in retry_data:
            if isinstance(retry_data[key], str):
                retry_data[key] = retry_data[key] + correction_instruction
                break
                
        try:
            return chain.invoke(retry_data)
        except OutputParserException as e:
            logger.error(f"JSON Parsing failed on retry {retries}: {e}")
            last_error = e
            retries += 1
            
    # If we get here, all retries failed
    logger.error("All JSON extraction retries failed. Returning empty/fallback structure.")
    return {}


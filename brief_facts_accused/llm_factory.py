
import sys
import os

# Use centralized LLM service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.llm_service import get_llm as _get_llm, invoke_extraction_with_retry


def get_llm():
    """
    Returns an LLMService configured for extraction tasks (brief_facts_accused).
    Uses core/llm_service.py factory — model, temperature, context window
    are all driven by .env (LLM_MODEL_EXTRACTION, OLLAMA_HOST).
    """
    return _get_llm('extraction')


def get_langchain_llm():
    """
    Returns a ChatOllama instance via the centralized service.
    Use this when a LangChain chain (.pipe / LCEL) is needed.
    """
    return _get_llm('extraction').get_langchain_model()


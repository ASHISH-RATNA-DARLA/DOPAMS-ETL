
from langchain_ollama import ChatOllama
import config

def get_llm():
    """
    Returns a configured ChatOllama instance.
    """
    llm = ChatOllama(
        base_url=config.OLLAMA_BASE_URL,
        model=config.LLM_MODEL,
        temperature=0,  # Low temperature for extraction tasks
    )
    return llm



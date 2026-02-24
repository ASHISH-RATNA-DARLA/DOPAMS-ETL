
import os
from dotenv import load_dotenv

load_dotenv()

# Database Config
DB_NAME = os.getenv("DB_NAME", "dopamas_testingdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
ACCUSED_TABLE_NAME = os.getenv("ACCUSED_TABLE_NAME", "brief_facts_accused")

# LLM Configuration
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "http://localhost:11434/api")
# Available Models:
# - qwen2.5-coder:3b (Fastest, ~2GB) - SELECTED FOR SPEED
# - llama3.1:8b (Too slow on CPU)
# - qwen2.5-coder:14b (Too slow on CPU)
LLM_MODEL = os.getenv("LLM_MODEL_EXTRACTION", "deepseek-coder-v2:16b")
LLM_CONTEXT_WINDOW = int(os.getenv("LLM_CONTEXT_WINDOW", "8192"))


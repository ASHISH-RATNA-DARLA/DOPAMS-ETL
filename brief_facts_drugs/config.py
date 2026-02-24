
import os
from dotenv import load_dotenv

load_dotenv()

# Database Config
DB_NAME = os.getenv("DB_NAME", "dopamas_testingdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DRUG_TABLE_NAME = os.getenv("DRUG_TABLE_NAME", "brief_facts_drugs_test")

# LLM Configuration
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "http://localhost:11434/api")
# Recommended for CPU: qwen2.5-coder:7b or qwen2.5-coder:14b
# Recommended for Accuracy (Slow on CPU): qwen2.5-coder:32b
LLM_MODEL = os.getenv("LLM_MODEL_EXTRACTION", "deepseek-coder-v2:16b")
LLM_CONTEXT_WINDOW = int(os.getenv("LLM_CONTEXT_WINDOW", "16384")) # Increased to handle long reports



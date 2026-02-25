
import os
from dotenv import load_dotenv

load_dotenv()

# Database Config
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DRUG_TABLE_NAME = os.getenv("DRUG_TABLE_NAME")

# LLM Configuration
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT")
# Recommended for CPU: qwen2.5-coder:7b or qwen2.5-coder:14b
# Recommended for Accuracy (Slow on CPU): qwen2.5-coder:32b
LLM_MODEL = os.getenv("LLM_MODEL_EXTRACTION")
LLM_CONTEXT_WINDOW = int(os.getenv("LLM_CONTEXT_WINDOW")) # Increased to handle long reports



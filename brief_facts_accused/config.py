
import os
from dotenv import load_dotenv

load_dotenv()

# Database Config
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
ACCUSED_TABLE_NAME = os.getenv("ACCUSED_TABLE_NAME")

# LLM Configuration
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT")
# Available Models:
# - qwen2.5-coder:3b (Fastest, ~2GB) - SELECTED FOR SPEED
# - llama3.1:8b (Too slow on CPU)
# - qwen2.5-coder:14b (Too slow on CPU)
LLM_MODEL = os.getenv("LLM_MODEL_EXTRACTION")
LLM_CONTEXT_WINDOW = int(os.getenv("LLM_CONTEXT_WINDOW"))


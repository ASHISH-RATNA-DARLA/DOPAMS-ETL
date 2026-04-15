
from env_utils import first_env, load_repo_environment, resolve_db_config, resolve_table_name

load_repo_environment()

DB_CONFIG = resolve_db_config()
ACCUSED_TABLE_NAME = resolve_table_name('ACCUSED_TABLE_NAME', 'brief_facts_ai')

# LLM Configuration
LLM_ENDPOINT = first_env('LLM_ENDPOINT')
# Available Models:
# - qwen2.5-coder:3b (Fastest, ~2GB) - SELECTED FOR SPEED
# - llama3.1:8b (Too slow on CPU)
# - qwen2.5-coder:14b (Too slow on CPU)
LLM_MODEL = first_env('LLM_MODEL_EXTRACTION')
LLM_CONTEXT_WINDOW = int(first_env('LLM_CONTEXT_WINDOW', default='0'))


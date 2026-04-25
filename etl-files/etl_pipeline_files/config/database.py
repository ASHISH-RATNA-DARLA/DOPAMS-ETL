"""Database configuration from the shared env resolver."""

import sys
from pathlib import Path

# Ensure the repo root (where env_utils.py lives) is on sys.path so this
# module can be imported regardless of the working directory.
_REPO_ROOT = Path(__file__).resolve().parents[3]  # config/ -> etl_pipeline_files/ -> etl-files/ -> repo root
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from env_utils import load_repo_environment, resolve_db_config

load_repo_environment()


def get_db_config():
    """Return the resolved database configuration used by file ETL modules."""
    config = resolve_db_config()
    return {
        'host': config['host'],
        'port': config['port'],
        'database': config['dbname'],
        'user': config['user'],
        'password': config['password'],
    }


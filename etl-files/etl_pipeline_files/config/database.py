"""Database configuration from the shared env resolver."""

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
        'source': config['source'],
    }


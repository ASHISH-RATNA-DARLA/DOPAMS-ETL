import os
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class ETLConfig:
    """Centralized configuration for brief_facts_ai ETL with validation."""

    # LLM Worker Configuration
    parallel_llm_workers: int

    # Batch Processing Configuration
    batch_size: int
    batch_commit_size: int

    # Database Pool Configuration
    db_pool_min_conn: int
    db_pool_max_conn: int

    @classmethod
    def from_env(cls) -> 'ETLConfig':
        """Load and validate configuration from environment variables.

        Raises ValueError if required variables missing or invalid.
        Fail-fast pattern: errors on initialization, not later.
        """

        # LLM Workers - must match OLLAMA_NUM_PARALLEL on server
        try:
            parallel_llm_workers = int(os.environ['PARALLEL_LLM_WORKERS'])
            if parallel_llm_workers < 1:
                raise ValueError("PARALLEL_LLM_WORKERS must be >= 1")
        except KeyError:
            raise ValueError("Missing required env var: PARALLEL_LLM_WORKERS")
        except ValueError as e:
            raise ValueError(f"Invalid PARALLEL_LLM_WORKERS: {e}")

        # Batch Size - number of crimes fetched per batch
        try:
            batch_size = int(os.environ['BATCH_SIZE'])
            if batch_size < 1:
                raise ValueError("BATCH_SIZE must be >= 1")
        except KeyError:
            raise ValueError("Missing required env var: BATCH_SIZE")
        except ValueError as e:
            raise ValueError(f"Invalid BATCH_SIZE: {e}")

        # Batch Commit Size - how many crimes before committing
        try:
            batch_commit_size = int(os.environ['BATCH_COMMIT_SIZE'])
            if batch_commit_size < 1:
                raise ValueError("BATCH_COMMIT_SIZE must be >= 1")
            if batch_commit_size > batch_size:
                raise ValueError("BATCH_COMMIT_SIZE cannot exceed BATCH_SIZE")
        except KeyError:
            raise ValueError("Missing required env var: BATCH_COMMIT_SIZE")
        except ValueError as e:
            raise ValueError(f"Invalid BATCH_COMMIT_SIZE: {e}")

        # Database Pool Min Connections
        try:
            db_pool_min_conn = int(os.environ['DB_POOL_MIN_CONN'])
            if db_pool_min_conn < 1:
                raise ValueError("DB_POOL_MIN_CONN must be >= 1")
        except KeyError:
            raise ValueError("Missing required env var: DB_POOL_MIN_CONN")
        except ValueError as e:
            raise ValueError(f"Invalid DB_POOL_MIN_CONN: {e}")

        # Database Pool Max Connections
        try:
            db_pool_max_conn = int(os.environ['DB_POOL_MAX_CONN'])
            if db_pool_max_conn < db_pool_min_conn:
                raise ValueError("DB_POOL_MAX_CONN cannot be less than DB_POOL_MIN_CONN")
        except KeyError:
            raise ValueError("Missing required env var: DB_POOL_MAX_CONN")
        except ValueError as e:
            raise ValueError(f"Invalid DB_POOL_MAX_CONN: {e}")

        config = cls(
            parallel_llm_workers=parallel_llm_workers,
            batch_size=batch_size,
            batch_commit_size=batch_commit_size,
            db_pool_min_conn=db_pool_min_conn,
            db_pool_max_conn=db_pool_max_conn
        )

        logger.info(f"ETL Configuration loaded: {config}")
        return config


# Lazy singleton
_config_instance: Optional[ETLConfig] = None

def get_config() -> ETLConfig:
    """Get or create the singleton ETLConfig instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = ETLConfig.from_env()
    return _config_instance


def reset_config() -> None:
    """Reset singleton (for testing only)."""
    global _config_instance
    _config_instance = None

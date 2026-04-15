"""Shared environment-loading and configuration helpers for DOPAMS ETL."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, Optional, Union
from urllib.parse import urlparse

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent
_ENV_LOADED = False


def load_repo_environment(extra_candidates: Optional[Iterable[object]] = None) -> Optional[str]:
    """Load the first repo-scoped env file found.

    Search order:
    - DOPAMS_ENV_FILE, if set
    - .env.server in repo root
    - .env in repo root
    - any additional caller-provided candidates
    """
    global _ENV_LOADED

    if _ENV_LOADED:
        return None

    candidates = []
    override = os.getenv("DOPAMS_ENV_FILE")
    if override:
        candidates.append(Path(override).expanduser())

    candidates.extend(
        [
            REPO_ROOT / ".env.server",
            REPO_ROOT / ".env",
        ]
    )

    if extra_candidates:
        candidates.extend(Path(candidate).expanduser() for candidate in extra_candidates)

    checked = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in checked:
            continue
        checked.add(resolved)
        if resolved.is_file():
            load_dotenv(str(resolved), override=False)
            _ENV_LOADED = True
            return str(resolved)

    load_dotenv()
    _ENV_LOADED = True
    return None


def first_env(*names: str, default: Optional[str] = None) -> Optional[str]:
    load_repo_environment()
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def get_int_env(name: str, default: int) -> int:
    value = first_env(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_bool_env(name: str, default: bool = False) -> bool:
    value = first_env(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _source_fields(source: str) -> list[str]:
    if source == "DATABASE_URL":
        return ["DATABASE_URL"]
    return [f"{source}_HOST", f"{source}_PORT", f"{source}_DB", f"{source}_USER", f"{source}_PASSWORD"]


def _collect_source_values(source: str) -> Dict[str, Optional[str]]:
    if source == "DATABASE_URL":
        return {"DATABASE_URL": first_env("DATABASE_URL")}
    return {
        "host": first_env(f"{source}_HOST"),
        "port": first_env(f"{source}_PORT"),
        "dbname": first_env(f"{source}_DB"),
        "user": first_env(f"{source}_USER"),
        "password": first_env(f"{source}_PASSWORD"),
    }


def _normalize_db_values(source: str, values: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    if source == "DATABASE_URL":
        return values
    return {
        "host": values.get("host"),
        "port": values.get("port"),
        "dbname": values.get("dbname"),
        "user": values.get("user"),
        "password": values.get("password"),
    }


def _complete_db_sources() -> Dict[str, Dict[str, str]]:
    load_repo_environment()

    sources: Dict[str, Dict[str, str]] = {}

    def _maybe_add_source(source: str, required_keys: Dict[str, Optional[str]]) -> None:
        if all(required_keys.values()):
            sources[source] = {key: str(value) for key, value in required_keys.items() if value is not None}

    for source in ("POSTGRES", "DB", "RDS"):
        raw = _collect_source_values(source)
        _maybe_add_source(source, _normalize_db_values(source, raw))

    database_url = first_env("DATABASE_URL")
    if database_url:
        sources["DATABASE_URL"] = {"DATABASE_URL": database_url}

    return sources


def _partial_db_sources() -> Dict[str, list[str]]:
    load_repo_environment()
    partials: Dict[str, list[str]] = {}
    for source in ("POSTGRES", "DB", "RDS"):
        values = _normalize_db_values(source, _collect_source_values(source))
        present = [key for key, value in values.items() if value not in (None, "")]
        if present and len(present) != len(values):
            missing = [key for key, value in values.items() if value in (None, "")]
            partials[source] = missing
    return partials


def resolve_db_config() -> Dict[str, Union[str, int]]:
    """Resolve a single PostgreSQL connection config and trace the selected source."""
    partials = _partial_db_sources()

    source = None
    config: Dict[str, Union[str, int]] = {}

    postgres = _normalize_db_values("POSTGRES", _collect_source_values("POSTGRES"))
    db_alias = _normalize_db_values("DB", _collect_source_values("DB"))
    rds = _normalize_db_values("RDS", _collect_source_values("RDS"))

    def _complete(values: Dict[str, Optional[str]]) -> bool:
        return all(values.get(key) not in (None, "") for key in ("host", "port", "dbname", "user", "password"))

    candidates = [
        ("POSTGRES_*", postgres),
        ("DB_*", db_alias),
        ("RDS_*", rds),
    ]

    complete_candidates = [(label, values) for label, values in candidates if _complete(values)]

    database_url = first_env("DATABASE_URL")
    if database_url:
        url_parts = urlparse(database_url)
        url_config = {
            "host": url_parts.hostname,
            "port": str(url_parts.port) if url_parts.port else None,
            "dbname": url_parts.path.lstrip("/") or None,
            "user": url_parts.username,
            "password": url_parts.password,
        }
        if _complete(url_config):
            complete_candidates.append(("DATABASE_URL", url_config))

    if not complete_candidates:
        if partials:
            details = ", ".join(f"{source} missing {', '.join(missing)}" for source, missing in partials.items())
            raise ValueError(f"Partial database configuration detected: {details}")
        raise ValueError(
            "No valid database configuration found. Provide one complete source set: POSTGRES_*, DB_*, RDS_* or DATABASE_URL."
        )

    canonical_values = complete_candidates[0][1]
    for label, values in complete_candidates[1:]:
        if values != canonical_values:
            raise ValueError(
                f"Conflicting database configuration sources detected between {complete_candidates[0][0]} and {label}."
            )

    source = complete_candidates[0][0]
    config = {
        "host": canonical_values["host"],
        "dbname": canonical_values["dbname"],
        "database": canonical_values["dbname"],
        "user": canonical_values["user"],
        "password": canonical_values["password"],
        "port": int(canonical_values["port"]),
        "source": source,
    }

    if partials:
        details = ", ".join(f"{name} missing {', '.join(missing)}" for name, missing in partials.items())
        print(f"[CONFIG] Ignoring partial DB alias sources because a complete source was found: {details}")

    print(f"[CONFIG] Using DB config source: {config['source']}")
    print(
        f"[CONFIG] DB target: host={config['host']} dbname={config['dbname']} user={config['user']} port={config['port']}"
    )
    return config


def resolve_postgres_config() -> Dict[str, Union[str, int]]:
    return resolve_db_config()


def resolve_api_base_url(*names: str, default: Optional[str] = None) -> Optional[str]:
    return first_env(*names, default=default)


def resolve_table_name(env_key: str, default: str) -> str:
    value = first_env(env_key, default="")
    value = (value or "").strip()
    return value or default
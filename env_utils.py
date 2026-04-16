"""Shared environment-loading and configuration helpers for DOPAMS ETL."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Union
from urllib.parse import urlparse

from dotenv import dotenv_values, load_dotenv


REPO_ROOT = Path(__file__).resolve().parent
_ENV_LOADED = False
_ENV_FILE_PATH: Optional[str] = None
_ENV_FILE_VALUES: Dict[str, str] = {}
_STRICT_ENV_MODE = False

_STRICT_ENV_FLAG = "STRICT_ENV"
_SUBPROCESS_ENV_PASSTHROUGH = {
    "COMSPEC",
    "HOME",
    "LANG",
    "LD_LIBRARY_PATH",
    "LOGNAME",
    "OLDPWD",
    "PATH",
    "PATHEXT",
    "PWD",
    "PYTHONHOME",
    "PYTHONPATH",
    "SHELL",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SYSTEMROOT",
    "TEMP",
    "TERM",
    "TMP",
    "TMPDIR",
    "TZ",
    "USER",
    "VIRTUAL_ENV",
    "WINDIR",
}
_SUBPROCESS_ENV_PREFIXES = (
    "CUDA_",
    "HTTPS_",
    "HTTP_",
    "LC_",
    "NVIDIA_",
    "NO_PROXY",
    "PYTHON",
    "REQUESTS_",
    "SSL_",
    "http_",
    "https_",
    "no_proxy",
)
_DB_SOURCE_ALIASES = {
    "AUTO": "AUTO",
    "DB": "DB",
    "DB_*": "DB",
    "DATABASE_URL": "DATABASE_URL",
    "POSTGRES": "POSTGRES",
    "POSTGRES_*": "POSTGRES",
    "RDS": "RDS",
    "RDS_*": "RDS",
}


def _is_present(value: Optional[str]) -> bool:
    return value not in (None, "")


def _is_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _normalize_env_values(values: Mapping[str, Optional[str]]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in values.items():
        if value is None:
            continue
        normalized[str(key)] = str(value)
    return normalized


def _source_label(source: str) -> str:
    return "DATABASE_URL" if source == "DATABASE_URL" else f"{source}_*"


def _normalize_db_source(source: Optional[str]) -> Optional[str]:
    value = (source or "").strip().upper()
    if not value:
        return None
    normalized = _DB_SOURCE_ALIASES.get(value)
    if not normalized:
        raise ValueError(
            "Unsupported DB_CONFIG_SOURCE. Use one of: AUTO, POSTGRES, DB, RDS, DATABASE_URL."
        )
    return normalized


def _strict_mode_requested(file_values: Optional[Mapping[str, str]] = None) -> bool:
    if _is_truthy(os.getenv(_STRICT_ENV_FLAG)):
        return True
    if file_values and _is_truthy(file_values.get(_STRICT_ENV_FLAG)):
        return True
    return False


def load_repo_environment(extra_candidates: Optional[Iterable[object]] = None) -> Optional[str]:
    """Load the first repo-scoped env file found.

    Search order:
    - DOPAMS_ENV_FILE, if set
    - .env.server in repo root
    - .env in repo root
    - any additional caller-provided candidates

    When STRICT_ENV=true, file values override inherited process values.
    """
    global _ENV_FILE_PATH, _ENV_FILE_VALUES, _ENV_LOADED, _STRICT_ENV_MODE

    if _ENV_LOADED:
        return _ENV_FILE_PATH

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
            file_values = _normalize_env_values(dotenv_values(str(resolved)))
            strict_mode = _strict_mode_requested(file_values)
            load_dotenv(str(resolved), override=strict_mode)
            _ENV_FILE_PATH = str(resolved)
            _ENV_FILE_VALUES = file_values
            _STRICT_ENV_MODE = strict_mode
            _ENV_LOADED = True
            return _ENV_FILE_PATH

    strict_mode = _strict_mode_requested()
    load_dotenv(override=strict_mode)
    _ENV_FILE_PATH = None
    _ENV_FILE_VALUES = {}
    _STRICT_ENV_MODE = strict_mode
    _ENV_LOADED = True
    return None


def is_strict_env_mode() -> bool:
    load_repo_environment()
    return _STRICT_ENV_MODE


def get_loaded_env_file() -> Optional[str]:
    load_repo_environment()
    return _ENV_FILE_PATH


def _should_passthrough_env(name: str) -> bool:
    if name in _SUBPROCESS_ENV_PASSTHROUGH:
        return True
    return any(name.startswith(prefix) for prefix in _SUBPROCESS_ENV_PREFIXES)


def build_subprocess_env(base_env: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """Build a child-process environment for ETL subprocess execution."""
    load_repo_environment()

    source_env = {str(key): str(value) for key, value in dict(base_env or os.environ).items()}
    if not _STRICT_ENV_MODE:
        return source_env

    runtime_env: Dict[str, str] = {}
    for key, value in source_env.items():
        if _should_passthrough_env(key):
            runtime_env[key] = value

    runtime_env.update(_ENV_FILE_VALUES)

    if _ENV_FILE_PATH:
        runtime_env["DOPAMS_ENV_FILE"] = _ENV_FILE_PATH

    runtime_env[_STRICT_ENV_FLAG] = _ENV_FILE_VALUES.get(_STRICT_ENV_FLAG, "true")
    return runtime_env


def first_env(*names: str, default: Optional[str] = None) -> Optional[str]:
    load_repo_environment()

    if _STRICT_ENV_MODE:
        for name in names:
            if name == "DOPAMS_ENV_FILE" and _ENV_FILE_PATH:
                return _ENV_FILE_PATH
            if name == _STRICT_ENV_FLAG and _STRICT_ENV_MODE:
                return _ENV_FILE_VALUES.get(_STRICT_ENV_FLAG, "true")

            value = _ENV_FILE_VALUES.get(name)
            if _is_present(value):
                return value
        return default

    for name in names:
        value = os.getenv(name)
        if _is_present(value):
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


def get_float_env(name: str, default: float) -> float:
    value = first_env(name)
    if value in (None, ""):
        return default
    try:
        return float(value)
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


def _present_source_fields(source: str) -> list[str]:
    if source == "DATABASE_URL":
        return ["DATABASE_URL"] if _is_present(first_env("DATABASE_URL")) else []

    return [field for field in _source_fields(source) if _is_present(first_env(field))]


def _partial_db_sources() -> Dict[str, list[str]]:
    load_repo_environment()
    partials: Dict[str, list[str]] = {}
    for source in ("POSTGRES", "DB", "RDS"):
        present = _present_source_fields(source)
        fields = _source_fields(source)
        if present and len(present) != len(fields):
            partials[source] = [field for field in fields if field not in present]
    return partials


def _database_url_config() -> Optional[Dict[str, Optional[str]]]:
    database_url = first_env("DATABASE_URL")
    if not database_url:
        return None

    url_parts = urlparse(database_url)
    return {
        "host": url_parts.hostname,
        "port": str(url_parts.port) if url_parts.port else None,
        "dbname": url_parts.path.lstrip("/") or None,
        "user": url_parts.username,
        "password": url_parts.password,
    }


def resolve_db_config() -> Dict[str, Union[str, int]]:
    """Resolve a single PostgreSQL connection config and trace the selected source."""
    partials = _partial_db_sources()
    strict_mode = is_strict_env_mode()
    configured_source = _normalize_db_source(first_env("DB_CONFIG_SOURCE", default=""))

    if configured_source is None:
        configured_source = "POSTGRES" if strict_mode else "AUTO"

    postgres = _normalize_db_values("POSTGRES", _collect_source_values("POSTGRES"))
    db_alias = _normalize_db_values("DB", _collect_source_values("DB"))
    rds = _normalize_db_values("RDS", _collect_source_values("RDS"))
    url_config = _database_url_config()

    candidate_values = {
        "POSTGRES": postgres,
        "DB": db_alias,
        "RDS": rds,
    }
    if url_config:
        candidate_values["DATABASE_URL"] = url_config

    def _complete(values: Optional[Dict[str, Optional[str]]]) -> bool:
        if not values:
            return False
        return all(values.get(key) not in (None, "") for key in ("host", "port", "dbname", "user", "password"))

    if configured_source != "AUTO":
        canonical_values = candidate_values.get(configured_source)
        if not _complete(canonical_values):
            if configured_source in partials:
                missing = ", ".join(partials[configured_source])
                raise ValueError(
                    f"Selected database source {_source_label(configured_source)} is incomplete. Missing: {missing}"
                )
            raise ValueError(f"Selected database source {_source_label(configured_source)} is not configured.")

        if strict_mode:
            unexpected_sources = []
            for source in ("POSTGRES", "DB", "RDS", "DATABASE_URL"):
                if source == configured_source:
                    continue
                present = _present_source_fields(source)
                if present:
                    unexpected_sources.append(f"{_source_label(source)} ({', '.join(present)})")
            if unexpected_sources:
                raise ValueError(
                    "STRICT_ENV requires exactly one configured DB source. Remove these extra settings: "
                    + "; ".join(unexpected_sources)
                )

        source = _source_label(configured_source)
    else:
        candidates = [
            ("POSTGRES_*", postgres),
            ("DB_*", db_alias),
            ("RDS_*", rds),
        ]
        complete_candidates = [(label, values) for label, values in candidates if _complete(values)]

        if _complete(url_config):
            complete_candidates.append(("DATABASE_URL", url_config))

        if not complete_candidates:
            if partials:
                details = ", ".join(
                    f"{_source_label(source)} missing {', '.join(missing)}" for source, missing in partials.items()
                )
                raise ValueError(f"Partial database configuration detected: {details}")
            raise ValueError(
                "No valid database configuration found. Provide one complete source set: "
                "POSTGRES_*, DB_*, RDS_* or DATABASE_URL."
            )

        canonical_values = complete_candidates[0][1]
        for label, values in complete_candidates[1:]:
            if values != canonical_values:
                raise ValueError(
                    f"Conflicting database configuration sources detected between {complete_candidates[0][0]} and {label}."
                )

        source = complete_candidates[0][0]

        if partials:
            details = ", ".join(
                f"{_source_label(name)} missing {', '.join(missing)}" for name, missing in partials.items()
            )
            print(f"[CONFIG] Ignoring partial DB alias sources because a complete source was found: {details}")

    print(f"[CONFIG] Using DB config source: {source}")

    config = {
        "host": canonical_values["host"],
        "dbname": canonical_values["dbname"],
        "user": canonical_values["user"],
        "password": canonical_values["password"],
        "port": int(canonical_values["port"]),
    }

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

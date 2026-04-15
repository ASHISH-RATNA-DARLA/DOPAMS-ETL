import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import psycopg2

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from env_utils import get_bool_env, load_repo_environment, resolve_db_config


class PreflightError(Exception):
    """Raised when a mandatory preflight validation fails."""


@dataclass
class ProcessBlock:
    order: int
    name: Optional[str]
    commands: List[str]


REQUIRED_TABLES = [
    "crimes",
    "accused",
    "persons",
    "hierarchy",
    "brief_facts_ai",
    "etl_crime_processing_log",
]


def parse_input_file(file_path: str) -> List[Dict[str, object]]:
    if not os.path.exists(file_path):
        raise PreflightError(f"Configuration file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()

    processes: List[ProcessBlock] = []
    current_block: Optional[ProcessBlock] = None
    header_pattern = re.compile(r"^\[Order\s+(\d+)\]", re.IGNORECASE)

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        header_match = header_pattern.match(line)
        if header_match:
            if current_block is not None:
                processes.append(current_block)

            current_block = ProcessBlock(
                order=int(header_match.group(1)),
                name=None,
                commands=[],
            )
            continue

        if current_block is None:
            continue

        if current_block.name is None and _looks_like_name(line):
            current_block.name = line
            continue

        current_block.commands.append(line)

    if current_block is not None:
        processes.append(current_block)

    return [
        {
            "order": str(block.order),
            "name": block.name,
            "commands": block.commands,
        }
        for block in processes
    ]


def validate_execution_order(processes: List[Dict[str, object]]) -> None:
    orders = [int(process["order"]) for process in processes]

    if not orders:
        raise PreflightError("No process blocks found in input file.")

    if len(set(orders)) != len(orders):
        raise PreflightError("Duplicate [Order X] blocks detected in input configuration.")

    expected = list(range(min(orders), max(orders) + 1))
    missing = sorted(set(expected) - set(orders))

    if missing:
        raise PreflightError(
            f"Execution order is inconsistent. Missing order numbers: {missing}"
        )

    if orders != sorted(orders):
        raise PreflightError(
            "Execution order is not sequential in file order. Reorder blocks to ascending [Order X]."
        )


def validate_directories(processes: List[Dict[str, object]]) -> None:
    missing_directories = []

    for process in processes:
        for command in process.get("commands", []):
            if command.startswith("cd "):
                path = command[3:].strip()
                if not os.path.isdir(path):
                    missing_directories.append(path)

    if missing_directories:
        unique_missing = sorted(set(missing_directories))
        raise PreflightError(
            "Referenced directories do not exist: " + ", ".join(unique_missing)
        )


def validate_scripts(processes: List[Dict[str, object]]) -> None:
    missing_scripts = []

    for process in processes:
        working_dir = _extract_working_dir(process)
        process_name = process.get("name") or f"order_{process.get('order')}"

        for command in process.get("commands", []):
            script_path = _extract_script_path(command, working_dir)
            if script_path and not os.path.exists(script_path):
                missing_scripts.append(f"{process_name}:{script_path}")

    if missing_scripts:
        raise PreflightError(
            "Referenced scripts not found: " + ", ".join(sorted(set(missing_scripts)))
        )


def resolve_db_env(env_name: str) -> Dict[str, str]:
    resolved = resolve_db_config()
    return {
        "DB_HOST": str(resolved["host"]),
        "DB_PORT": str(resolved["port"]),
        "DB_NAME": str(resolved["dbname"]),
        "DB_USER": str(resolved["user"]),
        "DB_PASSWORD": str(resolved["password"]),
        "DB_SOURCE": str(resolved["source"]),
    }


def validate_db_connection(db_env: Dict[str, str]) -> None:
    try:
        connection = psycopg2.connect(
            host=db_env["DB_HOST"],
            port=int(db_env["DB_PORT"]),
            dbname=db_env["DB_NAME"],
            user=db_env["DB_USER"],
            password=db_env["DB_PASSWORD"],
            connect_timeout=10,
        )

        skip_schema_check = get_bool_env("ETL_PREFLIGHT_SKIP_SCHEMA_CHECK", False)

        if not skip_schema_check:
            validate_minimum_schema(connection)

        connection.close()
    except Exception as exc:
        raise PreflightError(f"Database connectivity check failed: {str(exc)}") from exc


def validate_minimum_schema(connection) -> None:
    """Validate fresh DB has minimum core schema for unified ETL execution."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """
        )
        existing_tables = {row[0] for row in cursor.fetchall()}

    missing = [name for name in REQUIRED_TABLES if name not in existing_tables]
    if missing:
        raise PreflightError(
            "Database schema is incomplete. Missing required public tables: "
            + ", ".join(missing)
            + ". Apply DB-schema.sql and unified_brief_facts_etl.sql before running ETL."
        )


def run_preflight(config_path: str, env_name: str) -> None:
    # Resolve .env from project root (one level up from etl_master)
    load_repo_environment()

    resolved = resolve_db_config()
    print(f"[CONFIG] Preflight DB target: host={resolved['host']} dbname={resolved['dbname']} user={resolved['user']} source={resolved['source']}")


    processes = parse_input_file(config_path)
    validate_execution_order(processes)
    validate_directories(processes)
    validate_scripts(processes)

    db_env = resolve_db_env(env_name)
    validate_db_connection(db_env)



def _looks_like_name(line: str) -> bool:
    return not (
        line.startswith("cd ")
        or line.startswith("source ")
        or line.startswith("python")
        or line.startswith("/")
        or line.startswith("./")
        or "=" in line
    )



def _extract_working_dir(process: Dict[str, object]) -> Optional[str]:
    for command in process.get("commands", []):
        if command.startswith("cd "):
            return command[3:].strip()
    return None



def _extract_script_path(command: str, working_dir: Optional[str]) -> Optional[str]:
    tokens = command.split()
    if not tokens:
        return None

    if tokens[0] in {"python", "python3"}:
        if len(tokens) >= 3 and tokens[1] == "-m":
            return None
        if len(tokens) >= 2 and tokens[1].endswith(".py"):
            return _resolve_path(tokens[1], working_dir)
        return None

    if tokens[0].endswith(".py") or tokens[0].endswith(".sh"):
        return _resolve_path(tokens[0], working_dir)

    return None



def _resolve_path(path: str, working_dir: Optional[str]) -> str:
    if os.path.isabs(path):
        return path
    if working_dir:
        return os.path.abspath(os.path.join(working_dir, path))
    return os.path.abspath(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preflight validation for master_etl.py")
    parser.add_argument("--config", default="input.txt", help="Path to process configuration file")
    parser.add_argument("--env", default="prod", help="Runtime environment name, e.g., prod")
    cli_args = parser.parse_args()

    try:
        run_preflight(cli_args.config, cli_args.env)
    except PreflightError as error:
        print(f"PRECHECK FAILED: {str(error)}")
        raise SystemExit(1)

    print("PRECHECK PASSED")

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

from preflight_check import (
    PreflightError,
    parse_input_file,
    run_preflight,
)


def build_logger() -> logging.Logger:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    preferred_log_dir = "/logs"
    fallback_log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

    log_dir = preferred_log_dir
    try:
        os.makedirs(preferred_log_dir, exist_ok=True)
    except OSError:
        log_dir = fallback_log_dir
        os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"etl_run_{timestamp}.log")

    logger = logging.getLogger("master_etl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    logger.info("Structured run log: %s", log_file)
    return logger


logger = build_logger()


class StepExecutionError(Exception):
    def __init__(self, command: str, original_error: Exception):
        super().__init__(f"command='{command}' error='{str(original_error)}'")
        self.command = command
        self.original_error = original_error


def validate_mo_seizures_wiring(processes, config_path):
    has_mo_seizure_loader = False

    for process in processes:
        commands = process.get("commands", [])
        command_blob = " ".join(commands).lower()
        if "etl_mo_seizure.py" in command_blob and "etl_mo_seizures" in command_blob:
            has_mo_seizure_loader = True
            break

    if has_mo_seizure_loader:
        logger.info(
            "Orchestration hardening: corrected MO Seizures loader is explicitly wired in %s",
            config_path,
        )
    else:
        logger.warning(
            "Orchestration hardening: corrected MO Seizures loader was NOT found in %s",
            config_path,
        )


def normalize_processes_for_unified_mode(processes):
    normalized = []
    found_unified_block = False

    for process in processes:
        process_name = (process.get("name") or "").strip().lower()

        if process_name == "brief_facts_ai":
            normalized.append(process)
            found_unified_block = True
            continue

        if process_name == "brief_facts_accused":
            unified_block = dict(process)
            unified_block["name"] = "brief_facts_ai"
            normalized.append(unified_block)
            found_unified_block = True
            continue

        if process_name in {"brief_facts_drugs", "drug_standardization"}:
            logger.info(
                "Unified brief_facts_ai mode: skipping legacy block [Order %s: %s]",
                process.get("order"),
                process.get("name"),
            )
            continue

        normalized.append(process)

    if not found_unified_block:
        logger.warning(
            "Unified brief_facts_ai mode is enabled, but no brief_facts_ai/brief_facts_accused block was found"
        )

    return normalized


def validate_brief_facts_ai_wiring(processes, config_path):
    has_unified_block = any((process.get("name") or "").strip().lower() == "brief_facts_ai" for process in processes)
    has_legacy_accused = any((process.get("name") or "").strip().lower() == "brief_facts_accused" for process in processes)

    if has_unified_block or has_legacy_accused:
        logger.info("Unified brief_facts_ai orchestration enabled")
    else:
        logger.warning(
            "Unified brief_facts_ai orchestration enabled, but no matching block was found in %s",
            config_path,
        )


def optimize_refresh_steps(processes):
    refresh_indexes = []

    for idx, process in enumerate(processes):
        process_name = (process.get("name") or "").strip().lower()
        command_blob = " ".join(process.get("commands", [])).lower()
        if process_name == "refresh_views" or "views_refresh_sql.py" in command_blob:
            refresh_indexes.append(idx)

    if len(refresh_indexes) <= 1:
        return processes

    keep_index = refresh_indexes[-1]
    optimized = []

    refresh_process = processes[keep_index]

    for idx, process in enumerate(processes):
        if idx in refresh_indexes and idx != keep_index:
            logger.info(
                "Removing duplicate refresh step [Order %s: %s]; refresh will run once at pipeline end",
                process.get("order"),
                process.get("name"),
            )
            continue
        if idx == keep_index:
            continue
        optimized.append(process)

    optimized.append(refresh_process)

    return optimized


def extract_execution_context(process):
    working_dir = None
    runtime_env = os.environ.copy()
    executable_commands = []

    for raw_cmd in process.get("commands", []):
        cmd = raw_cmd.strip()
        if not cmd:
            continue

        if cmd.startswith("cd "):
            working_dir = cmd[3:].strip()
            continue

        if cmd.startswith("source "):
            activation_script = cmd[7:].strip()
            if activation_script.endswith("/bin/activate"):
                venv_root = os.path.dirname(os.path.dirname(activation_script))
                venv_bin = os.path.join(venv_root, "bin")
                runtime_env["VIRTUAL_ENV"] = venv_root
                runtime_env["PATH"] = f"{venv_bin}:{runtime_env.get('PATH', '')}"
                logger.info("Virtual environment detected: %s", venv_root)
            else:
                logger.warning("Unsupported source command retained as executable step: %s", cmd)
                executable_commands.append(cmd)
            continue

        executable_commands.append(cmd)

    return working_dir, runtime_env, executable_commands


def run_command(command, cwd, env):
    # Linux-targeted orchestration: execute in bash without shell chaining.
    subprocess.run(
        ["/bin/bash", "-lc", command],
        cwd=cwd,
        env=env,
        check=True,
        text=True,
    )


def run_process_once(process):
    order = process["order"]
    name = process.get("name") or "Unnamed Process"
    cwd, env, commands = extract_execution_context(process)

    if not commands:
        logger.warning("[Order %s: %s] has no executable commands; skipping", order, name)
        return

    for idx, command in enumerate(commands, start=1):
        logger.info("[Order %s: %s] command %d start: %s", order, name, idx, command)
        try:
            run_command(command, cwd, env)
        except Exception as exc:
            raise StepExecutionError(command, exc) from exc
        logger.info("[Order %s: %s] command %d success", order, name, idx)


def run_process_with_retry(process, process_index, max_retries=2):
    order = process["order"]
    name = process.get("name") or "Unnamed Process"
    retry_delays = [2, 5]
    attempts = max_retries + 1
    last_error = None

    for attempt in range(1, attempts + 1):
        process_start = time.time()
        logger.info("Step %d [Order %s: %s] started (attempt %d/%d)", process_index, order, name, attempt, attempts)

        try:
            run_process_once(process)
            duration = time.time() - process_start
            logger.info(
                "Step %d [Order %s: %s] ended | duration=%.2fs | status=SUCCESS",
                process_index,
                order,
                name,
                duration,
            )
            return True
        except Exception as exc:
            duration = time.time() - process_start
            last_error = exc
            logger.error(
                "Step %d [Order %s: %s] ended | duration=%.2fs | status=FAILED",
                process_index,
                order,
                name,
                duration,
            )
            logger.error(
                "FAILED STEP => number=%d, script=%s, error=%s",
                process_index,
                name,
                str(exc),
            )
            if isinstance(exc, StepExecutionError):
                logger.error("FAILED COMMAND => %s", exc.command)

            if attempt <= max_retries:
                delay = retry_delays[attempt - 1]
                logger.info(
                    "Retrying step %d [Order %s: %s] in %d seconds",
                    process_index,
                    order,
                    name,
                    delay,
                )
                time.sleep(delay)

    logger.error(
        "Step %d [Order %s: %s] exhausted retries and failed permanently: %s",
        process_index,
        order,
        name,
        str(last_error),
    )
    return False


def resolve_config_path(args):
    """Prefer --config, but keep --input-file as backward-compatible alias."""
    if args.config:
        return args.config
    if args.input_file:
        logger.warning("--input-file is deprecated; use --config going forward")
        return args.input_file
    return "input.txt"


def filter_processes_by_order(processes, start_order=None, end_order=None):
    """Optionally run a subset of ordered blocks for resume/debug compatibility."""
    if start_order is None and end_order is None:
        return processes

    filtered = []
    for process in processes:
        order = int(process.get("order"))
        if start_order is not None and order < start_order:
            continue
        if end_order is not None and order > end_order:
            continue
        filtered.append(process)

    if not filtered:
        raise ValueError(
            f"No processes found for requested order window start={start_order}, end={end_order}"
        )

    return filtered


def main():
    parser = argparse.ArgumentParser(description="Master ETL Orchestrator (Ubuntu-safe)")
    parser.add_argument("--config", default=None, help="Path to process configuration file")
    parser.add_argument("--input-file", dest="input_file", default=None, help="Deprecated alias for --config")
    parser.add_argument("--env", default="prod", help="Runtime environment name, e.g., prod")
    parser.add_argument("--start-order", type=int, default=None, help="Optional first order to execute")
    parser.add_argument("--end-order", type=int, default=None, help="Optional last order to execute")
    args = parser.parse_args()

    config_path = resolve_config_path(args)

    if args.start_order is not None and args.end_order is not None and args.start_order > args.end_order:
        logger.error("Invalid order range: --start-order cannot be greater than --end-order")
        sys.exit(1)

    if os.name != "posix":
        logger.error("This orchestrator is Linux-only and is intended for Ubuntu execution.")
        sys.exit(1)

    logger.info("Starting Master ETL Orchestrator")
    logger.info(
        "Using config=%s env=%s start_order=%s end_order=%s",
        config_path,
        args.env,
        args.start_order,
        args.end_order,
    )

    try:
        run_preflight(config_path, args.env)
    except PreflightError as exc:
        logger.error("Preflight failed: %s", str(exc))
        sys.exit(1)

    processes = parse_input_file(config_path)
    validate_mo_seizures_wiring(processes, config_path)
    processes = normalize_processes_for_unified_mode(processes)
    validate_brief_facts_ai_wiring(processes, config_path)
    processes = optimize_refresh_steps(processes)

    try:
        processes = filter_processes_by_order(processes, args.start_order, args.end_order)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    if not processes:
        logger.error("No process blocks found in configuration file. Ensure blocks start with [Order X].")
        sys.exit(1)

    logger.info("Found %d processes to execute.", len(processes))

    for process_index, process in enumerate(processes, start=1):
        if not run_process_with_retry(process, process_index=process_index, max_retries=2):
            logger.error("Master ETL execution stopped due to step failure.")
            sys.exit(1)

    logger.info("All ETL processes finished successfully.")


if __name__ == "__main__":
    main()


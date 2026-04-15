#!/usr/bin/env python3
"""Validate repository environment configuration before ETL runs."""

from __future__ import annotations

import sys

from env_utils import load_repo_environment, resolve_db_config


def main() -> int:
    load_repo_environment()
    config = resolve_db_config()
    print(f"[CONFIG] Using DB config source: {config['source']}")
    print(
        f"[CONFIG] DB target: host={config['host']} dbname={config['dbname']} user={config['user']} port={config['port']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

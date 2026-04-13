#!/bin/bash

set -euo pipefail

RESET_DB=false
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset-db)
      RESET_DB=true
      shift
      ;;
    --yes)
      ASSUME_YES=true
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--reset-db] [--yes]"
      exit 1
      ;;
  esac
done

ROOT_DIR="/data-drive/etl-process-dev"
MASTER_DIR="$ROOT_DIR/etl_master"
SCHEMA_SQL="$ROOT_DIR/DB-schema.sql"
UNIFIED_SQL="$ROOT_DIR/unified_brief_facts_etl.sql"
LOG_DIR="$ROOT_DIR/logs"
RUN_TS=$(date +"%Y%m%d_%H%M%S")
RUN_LOG_FILE="$LOG_DIR/fresh_db_run_${RUN_TS}.log"

if [[ ! -f "$SCHEMA_SQL" ]]; then
  echo "ERROR: Missing schema file: $SCHEMA_SQL"
  exit 1
fi

if [[ ! -f "$UNIFIED_SQL" ]]; then
  echo "ERROR: Missing unified schema file: $UNIFIED_SQL"
  exit 1
fi

mkdir -p "$LOG_DIR"
exec > >(tee -a "$RUN_LOG_FILE") 2>&1
echo "Logging fresh DB run output to: $RUN_LOG_FILE"

source "$ROOT_DIR/venv/bin/activate"

if [[ -f "$ROOT_DIR/.env.server" ]]; then
  set -a
  source "$ROOT_DIR/.env.server"
  set +a
elif [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
else
  echo "ERROR: Missing .env.server/.env in $ROOT_DIR"
  exit 1
fi

for var in DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD; do
  if [[ -z "${!var:-}" ]]; then
    echo "ERROR: $var is not set"
    exit 1
  fi
done

export PGPASSWORD="$DB_PASSWORD"
PSQL=(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1)

cd "$ROOT_DIR"

if [[ "$RESET_DB" == "true" ]]; then
  echo "WARNING: You are about to DELETE ALL DATA in database '$DB_NAME' on $DB_HOST:$DB_PORT"
  echo "This will drop and recreate public schema, then rebuild from SQL files."

  if [[ "$ASSUME_YES" != "true" ]]; then
    read -r -p "Type RESET to continue: " CONFIRM
    if [[ "$CONFIRM" != "RESET" ]]; then
      echo "Abort: reset confirmation did not match."
      exit 1
    fi
  fi

  echo "[0/5] Dropping and recreating public schema..."
  "${PSQL[@]}" -c "DROP SCHEMA public CASCADE;"
  "${PSQL[@]}" -c "CREATE SCHEMA public;"
  "${PSQL[@]}" -c "GRANT ALL ON SCHEMA public TO PUBLIC;"
fi

echo "[1/5] Applying base schema..."
"${PSQL[@]}" -f "$SCHEMA_SQL"

echo "[2/5] Applying unified brief_facts_ai schema updates (if needed)..."
BRIEF_FACTS_AI_EXISTS=$("${PSQL[@]}" -tAc "SELECT to_regclass('public.brief_facts_ai') IS NOT NULL;")
if [[ "$BRIEF_FACTS_AI_EXISTS" == "t" ]]; then
  echo "brief_facts_ai already exists from base schema; skipping unified_brief_facts_etl.sql"
else
  "${PSQL[@]}" -f "$UNIFIED_SQL"
fi

echo "[3/5] Running preflight checks..."
cd "$MASTER_DIR"
python3 preflight_check.py --config input.txt --env prod

echo "[4/5] Running unified ETL..."
python3 master_etl.py --config input.txt --env prod

echo "[5/5] Completed"
echo "Fresh DB unified ETL run completed successfully."
echo "Run log saved at: $RUN_LOG_FILE"

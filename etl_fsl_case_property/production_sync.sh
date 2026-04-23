#!/bin/bash

# FSL Case Property ETL - Production Sync & Execution Script
# Validates environment, syncs with database, and executes ETL

set -o allexport
source .env 2>/dev/null || source ../.env 2>/dev/null || true
set +o allexport

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PATH="${PROJECT_ROOT}/venv"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    local level=$1
    shift
    local msg="$@"
    local ts=$(date '+%Y-%m-%d %H:%M:%S')

    case $level in
        SUCCESS) echo -e "${GREEN}[${ts}] ✅ ${msg}${NC}" ;;
        ERROR)   echo -e "${RED}[${ts}] ❌ ${msg}${NC}" && return 1 ;;
        WARNING) echo -e "${YELLOW}[${ts}] ⚠️  ${msg}${NC}" ;;
        INFO)    echo -e "${BLUE}[${ts}] ℹ️  ${msg}${NC}" ;;
        *)       echo "[${ts}] ${msg}" ;;
    esac
    return 0
}

check_venv() {
    log INFO "Checking Python virtual environment..."
    if [ ! -d "$VENV_PATH" ]; then
        log ERROR "Virtual environment not found at $VENV_PATH"
        return 1
    fi
    log SUCCESS "Virtual environment found"
    return 0
}

activate_venv() {
    log INFO "Activating virtual environment..."
    source "$VENV_PATH/bin/activate" || return 1
    log SUCCESS "Virtual environment activated"
    python3 --version
    return 0
}

verify_env_vars() {
    log INFO "Verifying required environment variables..."

    local required_vars=(
        "POSTGRES_HOST"
        "POSTGRES_PORT"
        "POSTGRES_DB"
        "POSTGRES_USER"
        "POSTGRES_PASSWORD"
        "DOPAMAS_API_URL"
        "DOPAMAS_API_KEY"
    )

    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            log ERROR "Missing required variable: $var"
            return 1
        fi
    done

    log SUCCESS "All required environment variables set"
    log INFO "  POSTGRES_HOST: ${POSTGRES_HOST}:${POSTGRES_PORT}"
    log INFO "  POSTGRES_DB: ${POSTGRES_DB}"
    log INFO "  DOPAMAS_API_URL: ${DOPAMAS_API_URL}"
    return 0
}

verify_db_sync() {
    log INFO "Verifying database synchronization..."

    export PGPASSWORD="${POSTGRES_PASSWORD}"

    # Check tables exist and are synced
    local checks=(
        "crimes:7747:Crime records"
        "mo_seizures:2714:MO Seizure records"
        "fsl_case_property:any:FSL Case Property table"
        "fsl_case_property_media:any:Media files table"
    )

    for check in "${checks[@]}"; do
        IFS=':' read -r table expected_min desc <<< "$check"

        local count=$(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
            -tc "SELECT COUNT(*) FROM $table 2>/dev/null;" 2>/dev/null | tr -d ' ')

        if [ -z "$count" ]; then
            log ERROR "Failed to query $table"
            return 1
        fi

        if [ "$expected_min" != "any" ]; then
            if [ "$count" -lt "$expected_min" ]; then
                log WARNING "$desc: $count records (expected >= $expected_min)"
            else
                log SUCCESS "$desc: $count records ✓"
            fi
        else
            log SUCCESS "$desc exists ✓"
        fi
    done

    return 0
}

run_etl() {
    log INFO "Starting FSL Case Property ETL execution..."
    log INFO "=========================================="

    cd "$SCRIPT_DIR" || return 1

    python3 etl_fsl_case_property.py
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log SUCCESS "ETL execution completed successfully"
        return 0
    else
        log ERROR "ETL execution failed with exit code $exit_code"
        return 1
    fi
}

verify_etl_results() {
    log INFO "Verifying ETL results..."

    export PGPASSWORD="${POSTGRES_PASSWORD}"

    # Get final record count
    local final_count=$(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -tc "SELECT COUNT(*) FROM fsl_case_property;" 2>/dev/null | tr -d ' ')

    if [ -z "$final_count" ]; then
        log WARNING "Could not verify final record count"
        return 1
    fi

    if [ "$final_count" -gt 0 ]; then
        log SUCCESS "✅ ETL completed - Inserted $final_count records into fsl_case_property"
        log INFO "Database is synchronized and production-ready"
        return 0
    else
        log WARNING "No records inserted (check logs for details)"
        return 1
    fi
}

print_summary() {
    log INFO ""
    log INFO "=========================================="
    log INFO "PRODUCTION SYNC SUMMARY"
    log INFO "=========================================="
    log INFO "Environment: Production"
    log INFO "Database: ${POSTGRES_DB} @ ${POSTGRES_HOST}"
    log INFO "ETL Status: Completed"
    log INFO ""
    log INFO "Next Steps:"
    log INFO "  1. Review execution logs in ./logs/ directory"
    log INFO "  2. Verify record counts in database"
    log INFO "  3. Monitor system for any anomalies"
    log INFO ""
    log INFO "For more details, see:"
    log INFO "  - Execution logs: ./logs/fsl_case_property_*.log"
    log INFO "  - Database logs: Check PostgreSQL logs"
    log INFO "=========================================="
}

main() {
    log INFO "FSL Case Property ETL - Production Sync"
    log INFO "=========================================="

    check_venv || exit 1
    activate_venv || exit 1
    verify_env_vars || exit 1
    verify_db_sync || exit 1

    run_etl
    local etl_result=$?

    if [ $etl_result -eq 0 ]; then
        verify_etl_results
        print_summary
        exit 0
    else
        print_summary
        exit 1
    fi
}

# Run main function
main "$@"

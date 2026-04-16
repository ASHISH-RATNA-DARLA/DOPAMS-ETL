#!/bin/bash

################################################################################
# ETL Test Runner - Executes each ETL in master order with logging
# Usage: ./test_each_etl.sh
# Runs from: /data-drive/etl-process-dev
# Logs saved in: Each ETL's root folder as etl_test_TIMESTAMP.log
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="/data-drive/etl-process-dev"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MASTER_LOG_DIR="${PROJECT_ROOT}/test_logs_${TIMESTAMP}"

# ETL Steps in master order (from input.txt logic)
# Add each ETL step as: "name:path:command"
ETL_STEPS=(
    "crimes:etl-crimes:python3 etl_crimes.py"
    "accused:etl-accused:python3 etl_accused.py"
    "persons:etl-persons:python3 etl_persons.py"
    "disposal:etl-disposal:python3 etl_disposal.py"
    "arrests:etl-arrests:python3 etl_arrests.py"
    "mo_seizures:etl-mo-seizures:python3 etl_mo_seizures.py"
    "chargesheet:etl-chargesheet:python3 etl_chargesheet.py"
    "interrogation_reports:etl-ir:python3 ir_etl.py"
    "brief_facts_ai:etl-brief-facts-ai:python3 etl_brief_facts_ai.py"
    "properties:etl-properties:python3 etl_properties.py"
)

# Statistics tracking
TOTAL_STEPS=${#ETL_STEPS[@]}
PASSED_STEPS=0
FAILED_STEPS=0
declare -a FAILED_LIST
declare -a PASSED_LIST

################################################################################
# Functions
################################################################################

print_header() {
    echo -e "\n${BLUE}=================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=================================================================================${NC}\n"
}

print_step() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

setup_environment() {
    cd "$PROJECT_ROOT" || {
        print_error "Failed to change to project root: $PROJECT_ROOT"
        exit 1
    }

    # Create master log directory
    mkdir -p "$MASTER_LOG_DIR"

    print_header "ETL Test Suite Started"
    echo "Project Root: $PROJECT_ROOT"
    echo "Test Logs Dir: $MASTER_LOG_DIR"
    echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Total ETL Steps: $TOTAL_STEPS"
}

run_etl_step() {
    local step_name="$1"
    local step_dir="$2"
    local step_cmd="$3"
    local step_num="$4"

    print_step "[$step_num/$TOTAL_STEPS] Running: $step_name"

    # Create step-specific log file in its directory
    local step_log_dir="$PROJECT_ROOT/$step_dir"
    local step_log_file="$step_log_dir/etl_test_${TIMESTAMP}.log"

    # Ensure directory exists
    mkdir -p "$step_log_dir"

    # Also copy to master log directory
    local master_log_file="$MASTER_LOG_DIR/${step_name}_test_${TIMESTAMP}.log"

    echo "Log Location: $step_log_file" >&2

    (
        {
            echo "================================================================================"
            echo "ETL Step: $step_name"
            echo "Step Directory: $step_dir"
            echo "Command: $step_cmd"
            echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "================================================================================"
            echo ""

            # Activate venv if it exists
            if [ -f "$step_dir/venv/bin/activate" ]; then
                echo "[INFO] Activating virtual environment..."
                source "$step_dir/venv/bin/activate"
                echo "[INFO] Python: $(which python3)"
                echo "[INFO] Pip: $(which pip)"
            elif [ -f "./venv/bin/activate" ]; then
                echo "[INFO] Activating main virtual environment..."
                source "./venv/bin/activate"
                echo "[INFO] Python: $(which python3)"
            else
                echo "[WARN] No virtual environment found"
            fi

            echo ""
            echo "Running command: $step_cmd"
            echo "================================================================================"

            # Change to step directory and run command
            cd "$PROJECT_ROOT/$step_dir"
            eval "$step_cmd"

            echo ""
            echo "================================================================================"
            echo "Step Status: SUCCESS"
            echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "================================================================================"

        } 2>&1
    ) | tee "$step_log_file" | tee -a "$master_log_file"

    # Capture exit status
    local exit_status=${PIPESTATUS[0]}

    return $exit_status
}

print_summary() {
    print_header "ETL Test Suite Summary"

    echo "Total Steps: $TOTAL_STEPS"
    echo -e "${GREEN}Passed: $PASSED_STEPS${NC}"
    echo -e "${RED}Failed: $FAILED_STEPS${NC}"
    echo ""

    if [ $PASSED_STEPS -gt 0 ]; then
        echo -e "${GREEN}Passed Steps:${NC}"
        for step in "${PASSED_LIST[@]}"; do
            echo "  ✓ $step"
        done
        echo ""
    fi

    if [ $FAILED_STEPS -gt 0 ]; then
        echo -e "${RED}Failed Steps:${NC}"
        for step in "${FAILED_LIST[@]}"; do
            echo "  ✗ $step"
        done
        echo ""
    fi

    echo "Master Log Directory: $MASTER_LOG_DIR"
    echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')"

    echo ""
    echo "Log Files:"
    ls -lh "$MASTER_LOG_DIR"/
}

################################################################################
# Main Execution
################################################################################

main() {
    setup_environment

    # Run each ETL step
    for i in "${!ETL_STEPS[@]}"; do
        IFS=':' read -r step_name step_dir step_cmd <<< "${ETL_STEPS[$i]}"
        step_num=$((i + 1))

        if run_etl_step "$step_name" "$step_dir" "$step_cmd" "$step_num"; then
            print_success "$step_name completed"
            PASSED_STEPS=$((PASSED_STEPS + 1))
            PASSED_LIST+=("$step_name")
        else
            print_error "$step_name failed (exit code: $?)"
            FAILED_STEPS=$((FAILED_STEPS + 1))
            FAILED_LIST+=("$step_name")

            # Option: Continue on failure or stop
            # Uncomment to stop on first failure:
            # break
        done

        echo ""
    done

    # Print summary
    print_summary

    # Exit with error if any steps failed
    if [ $FAILED_STEPS -gt 0 ]; then
        echo -e "\n${RED}Test suite completed with failures!${NC}"
        exit 1
    else
        echo -e "\n${GREEN}All tests passed!${NC}"
        exit 0
    fi
}

# Run main function
main "$@"

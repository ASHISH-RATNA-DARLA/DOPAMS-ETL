#!/bin/bash

################################################################################
# ETL Test Runner - Reads from input.txt and runs each step with logging
# Usage: ./test_etl_from_config.sh [config_file]
# Default config: ./input.txt
# Runs from: /data-drive/etl-process-dev
# Logs saved: Each ETL's root folder + master test_logs directory
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="${1:-.}"
CONFIG_FILE="${2:-.}/input.txt"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MASTER_LOG_DIR="${PROJECT_ROOT}/test_logs_${TIMESTAMP}"
STEP_COUNTER=0
TOTAL_STEPS=0
PASSED_STEPS=0
FAILED_STEPS=0
declare -a FAILED_LIST=()
declare -a PASSED_LIST=()
declare -a STEP_TIMES=()

# Validate config file
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: Config file not found: $CONFIG_FILE${NC}"
    exit 1
fi

################################################################################
# Functions
################################################################################

print_header() {
    echo -e "\n${BLUE}╔════════════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║ $1${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════════════════╝${NC}\n"
}

print_step_header() {
    echo -e "\n${CYAN}─────────────────────────────────────────────────────────────────────────────${NC}"
    echo -e "${CYAN}Step [$1/$2]: $3${NC}"
    echo -e "${CYAN}─────────────────────────────────────────────────────────────────────────────${NC}\n"
}

print_info() {
    echo -e "${CYAN}[$(date '+%H:%M:%S')] ℹ $1${NC}"
}

print_success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓ $1${NC}"
}

print_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠ $1${NC}"
}

parse_config() {
    print_header "Parsing Configuration: $CONFIG_FILE"

    local current_order=""
    local current_name=""
    local current_cwd=""
    local commands=()

    while IFS= read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" ]] && continue
        [[ "$line" =~ ^# ]] && continue

        # Parse [Order X] blocks
        if [[ "$line" =~ ^\[Order\ ([0-9]+)\] ]]; then
            # If we have a previous block, save it
            if [ -n "$current_order" ] && [ ${#commands[@]} -gt 0 ]; then
                TOTAL_STEPS=$((TOTAL_STEPS + 1))
                echo "  Order $current_order: $current_name (${#commands[@]} commands)"
            fi

            current_order="${BASH_REMATCH[1]}"
            current_cwd=""
            commands=()
            current_name=$(echo "$line" | sed 's/\[Order [0-9]*\] //')
            print_info "Found Order $current_order: $current_name"

        # Parse Name: blocks
        elif [[ "$line" =~ ^Name: ]]; then
            current_name=$(echo "$line" | sed 's/^Name: //')

        # Parse Cd: blocks (working directory)
        elif [[ "$line" =~ ^Cd: ]]; then
            current_cwd=$(echo "$line" | sed 's/^Cd: //')

        # Collect commands
        elif [ -n "$current_order" ]; then
            commands+=("$line")
        fi

    done < "$CONFIG_FILE"

    # Don't forget the last block
    if [ -n "$current_order" ] && [ ${#commands[@]} -gt 0 ]; then
        TOTAL_STEPS=$((TOTAL_STEPS + 1))
        echo "  Order $current_order: $current_name (${#commands[@]} commands)"
    fi

    echo ""
    print_success "Configuration parsed: $TOTAL_STEPS ETL steps found"
}

setup_environment() {
    cd "$PROJECT_ROOT" || {
        print_error "Failed to change to project root: $PROJECT_ROOT"
        exit 1
    }

    # Create master log directory
    mkdir -p "$MASTER_LOG_DIR"

    print_header "ETL Test Suite"
    echo "Project Root:     $PROJECT_ROOT"
    echo "Config File:      $CONFIG_FILE"
    echo "Master Log Dir:   $MASTER_LOG_DIR"
    echo "Start Time:       $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Total ETL Steps:  $TOTAL_STEPS"
    echo ""
}

run_etl_steps() {
    print_header "Running ETL Steps"

    local step_number=0
    local current_order=""
    local current_name=""
    local current_cwd=""
    local commands=()

    while IFS= read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" ]] && continue
        [[ "$line" =~ ^# ]] && continue

        # Parse [Order X] blocks
        if [[ "$line" =~ ^\[Order\ ([0-9]+)\] ]]; then
            # Execute previous block if exists
            if [ -n "$current_order" ] && [ ${#commands[@]} -gt 0 ]; then
                step_number=$((step_number + 1))
                execute_etl_step "$step_number" "$current_order" "$current_name" "$current_cwd" "${commands[@]}"
            fi

            current_order="${BASH_REMATCH[1]}"
            current_cwd=""
            commands=()
            current_name=$(echo "$line" | sed 's/\[Order [0-9]*\] //')

        # Parse Name: blocks
        elif [[ "$line" =~ ^Name: ]]; then
            current_name=$(echo "$line" | sed 's/^Name: //')

        # Parse Cd: blocks (working directory)
        elif [[ "$line" =~ ^Cd: ]]; then
            current_cwd=$(echo "$line" | sed 's/^Cd: //')

        # Collect commands
        elif [ -n "$current_order" ]; then
            commands+=("$line")
        fi

    done < "$CONFIG_FILE"

    # Execute last block
    if [ -n "$current_order" ] && [ ${#commands[@]} -gt 0 ]; then
        step_number=$((step_number + 1))
        execute_etl_step "$step_number" "$current_order" "$current_name" "$current_cwd" "${commands[@]}"
    fi
}

execute_etl_step() {
    local step_num="$1"
    local order="$2"
    local name="$3"
    local working_dir="$4"
    shift 4
    local commands=("$@")

    print_step_header "$step_num" "$TOTAL_STEPS" "$name (Order $order)"

    # Determine working directory
    if [ -z "$working_dir" ]; then
        # Try to infer from command (if it's a python script)
        for cmd in "${commands[@]}"; do
            if [[ "$cmd" =~ python3\ ([a-zA-Z_]+\.py) ]]; then
                # Guess from script name - look for matching directory
                working_dir="."
                break
            fi
        done
    fi

    working_dir="${working_dir:-.}"

    # Create log file path
    local step_name=$(echo "$name" | tr ' ' '_' | tr '/' '_' | tr -d '()' | tr '[:upper:]' '[:lower:]')
    local step_log_file="${working_dir}/etl_test_${TIMESTAMP}.log"
    local master_log_file="$MASTER_LOG_DIR/${order}_${step_name}_${TIMESTAMP}.log"

    print_info "Working Directory: $working_dir"
    print_info "Log File: $step_log_file"

    # Create step directory if needed
    mkdir -p "$working_dir"

    local start_time=$(date +%s)

    # Execute the commands
    (
        {
            echo "================================================================================"
            echo "ETL Step: $name"
            echo "Order: $order"
            echo "Working Directory: $working_dir"
            echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "Commands: ${#commands[@]}"
            echo "================================================================================"
            echo ""

            # Change to working directory
            if ! cd "$working_dir"; then
                echo "[ERROR] Failed to change to directory: $working_dir"
                return 1
            fi

            # Check for venv and activate
            if [ -f "venv/bin/activate" ]; then
                print_info "Activating virtual environment..."
                source "venv/bin/activate"
                echo "[INFO] Python: $(which python3)"
                echo "[INFO] Python Version: $(python3 --version 2>&1)"
                echo ""
            elif [ -f "../venv/bin/activate" ]; then
                print_info "Activating parent virtual environment..."
                source "../venv/bin/activate"
                echo "[INFO] Python: $(which python3)"
                echo ""
            else
                echo "[WARN] No virtual environment found - using system Python"
                echo ""
            fi

            # Execute each command
            local cmd_num=1
            for cmd in "${commands[@]}"; do
                # Skip cd and source commands (already handled)
                if [[ "$cmd" =~ ^cd\ |^source\ ]]; then
                    echo "[SKIP] Directive already processed: $cmd"
                    continue
                fi

                echo "─────────────────────────────────────────────────────────────────────────────"
                echo "Command $cmd_num: $cmd"
                echo "─────────────────────────────────────────────────────────────────────────────"
                echo ""

                # Execute command
                if eval "$cmd"; then
                    echo ""
                    echo "[OK] Command completed successfully"
                else
                    local exit_code=$?
                    echo ""
                    echo "[ERROR] Command failed with exit code: $exit_code"
                    return $exit_code
                fi

                cmd_num=$((cmd_num + 1))
                echo ""
            done

            echo "================================================================================"
            echo "Step Status: SUCCESS"
            echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "================================================================================"

        } 2>&1
    ) | tee "$step_log_file" | tee -a "$master_log_file"

    # Capture exit status
    local exit_status=${PIPESTATUS[0]}

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    # Track result
    if [ $exit_status -eq 0 ]; then
        print_success "$name completed in ${duration}s"
        PASSED_STEPS=$((PASSED_STEPS + 1))
        PASSED_LIST+=("Order $order: $name")
        STEP_TIMES+=("$name: ${duration}s")
    else
        print_error "$name failed in ${duration}s (exit code: $exit_status)"
        FAILED_STEPS=$((FAILED_STEPS + 1))
        FAILED_LIST+=("Order $order: $name")
        STEP_TIMES+=("$name: ${duration}s (FAILED)")
    fi

    echo ""
    return $exit_status
}

print_summary() {
    print_header "Test Suite Summary"

    echo "Test Started:  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Total Steps:   $TOTAL_STEPS"
    echo -e "${GREEN}Passed:        $PASSED_STEPS${NC}"
    echo -e "${RED}Failed:        $FAILED_STEPS${NC}"
    echo ""

    if [ ${#STEP_TIMES[@]} -gt 0 ]; then
        echo "Step Execution Times:"
        for time_entry in "${STEP_TIMES[@]}"; do
            echo "  • $time_entry"
        done
        echo ""
    fi

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

    echo "Logs Directory: $MASTER_LOG_DIR"
    echo "Log Files:"
    ls -lh "$MASTER_LOG_DIR"/ | tail -n +2

    echo ""
}

################################################################################
# Main Execution
################################################################################

main() {
    print_info "Starting ETL Test Suite"

    # Parse configuration first (dry run to count steps)
    parse_config

    # Setup environment
    setup_environment

    # Run ETL steps
    run_etl_steps

    # Print summary
    print_summary

    # Exit with error if any steps failed
    if [ $FAILED_STEPS -gt 0 ]; then
        echo -e "\n${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${RED}Test suite completed with failures: $FAILED_STEPS/$TOTAL_STEPS${NC}"
        echo -e "${RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        exit 1
    else
        echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}All $TOTAL_STEPS tests passed!${NC}"
        echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        exit 0
    fi
}

# Run main
main "$@"

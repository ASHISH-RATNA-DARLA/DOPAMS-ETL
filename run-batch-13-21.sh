#!/bin/bash

# Batch runner for ETL Orders 13-21
# Runs all processes sequentially with logging

set -e  # Exit on first error

BASE_DIR="/data-drive/etl-process-dev"
VENV_ACTIVATE="source $BASE_DIR/venv/bin/activate"
LOG_DIR="$BASE_DIR/batch-logs"

# Create log directory
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "Starting ETL Batch Run (Orders 13-21)"
echo "Start Time: $(date)"
echo "=========================================="

# Order 13 – properties
echo ""
echo "[$(date)] Running Order 13 – properties..."
cd "$BASE_DIR/etl-properties" && $VENV_ACTIVATE && nohup python3 etl_properties.py > "$LOG_DIR/etl_properties.log" 2>&1 &
sleep 2

# Order 14 – IR
echo "[$(date)] Running Order 14 – IR..."
cd "$BASE_DIR/etl-ir" && $VENV_ACTIVATE && nohup python3 ir_etl.py > "$LOG_DIR/ir_etl.log" 2>&1 &
sleep 2

# Order 15 – Disposal
echo "[$(date)] Running Order 15 – Disposal..."
cd "$BASE_DIR/etl-disposal" && $VENV_ACTIVATE && nohup python3 etl_disposal.py > "$LOG_DIR/etl_disposal.log" 2>&1 &
sleep 2

# Order 16 – arrests
echo "[$(date)] Running Order 16 – arrests..."
cd "$BASE_DIR/etl_arrests" && $VENV_ACTIVATE && nohup python3 etl_arrests.py > "$LOG_DIR/etl_arrests.log" 2>&1 &
sleep 2

# Order 17 – mo_seizures
echo "[$(date)] Running Order 17 – mo_seizures..."
cd "$BASE_DIR/etl_mo_seizures" && $VENV_ACTIVATE && nohup python3 etl_mo_seizure.py > "$LOG_DIR/etl_mo_seizure.log" 2>&1 &
sleep 2

# Order 18 – chargesheets
echo "[$(date)] Running Order 18 – chargesheets..."
cd "$BASE_DIR/etl_chargesheets" && $VENV_ACTIVATE && nohup python3 etl_chargesheets.py > "$LOG_DIR/etl_chargesheets.log" 2>&1 &
sleep 2

# Order 19 – updated_chargesheet
echo "[$(date)] Running Order 19 – updated_chargesheet..."
cd "$BASE_DIR/etl_updated_chargesheet" && $VENV_ACTIVATE && nohup python3 etl_update_chargesheet.py > "$LOG_DIR/etl_update_chargesheet.log" 2>&1 &
sleep 2

# Order 20 – fsl_case_property
echo "[$(date)] Running Order 20 – fsl_case_property..."
cd "$BASE_DIR/etl_fsl_case_property" && $VENV_ACTIVATE && nohup python3 etl_fsl_case_property.py > "$LOG_DIR/etl_fsl_case_property.log" 2>&1 &
sleep 2

# Order 21 – refresh_views (1st)
echo "[$(date)] Running Order 21 – refresh_views (1st)..."
cd "$BASE_DIR/etl_refresh_views" && $VENV_ACTIVATE && nohup python3 views_refresh_sql.py > "$LOG_DIR/views_refresh_1.log" 2>&1 &

echo ""
echo "=========================================="
echo "All 9 processes launched!"
echo "Logs are available in: $LOG_DIR"
echo "=========================================="
echo ""
echo "To monitor processes, use:"
echo "  ps aux | grep python"
echo "  tail -f $LOG_DIR/*.log"

#!/bin/bash
# Script to diagnose the persons ETL failure on the remote server

echo "=== Checking Latest Persons ETL Execution Log ==="
LATEST_LOG=$(find /data-drive/etl-process-dev/etl_master/logs -type d -name "*" | sort -r | head -1)

if [ -z "$LATEST_LOG" ]; then
    echo "❌ No ETL logs found"
    exit 1
fi

PERSONS_LOG="$LATEST_LOG/persons/execution.log"

if [ ! -f "$PERSONS_LOG" ]; then
    echo "❌ Persons execution log not found: $PERSONS_LOG"
    ls -la "$LATEST_LOG/" 2>/dev/null || echo "Log directory not accessible"
    exit 1
fi

echo "📋 Latest log: $PERSONS_LOG"
echo ""
echo "=== ERROR SECTION (Last 100 lines) ==="
tail -100 "$PERSONS_LOG"

echo ""
echo "=== LOOKING FOR TRACEBACK ==="
grep -A 20 "Traceback\|Error\|Exception" "$PERSONS_LOG" | head -50

echo ""
echo "=== FILE INFO ==="
ls -lh "$PERSONS_LOG"
wc -l "$PERSONS_LOG"

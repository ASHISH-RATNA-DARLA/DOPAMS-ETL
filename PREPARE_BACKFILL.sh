#!/bin/bash
# Prepare database for ETL backfill from June 2022

set -e

echo "=========================================="
echo "ETL Backfill Preparation Script"
echo "=========================================="
echo ""

# Database credentials from environment
POSTGRES_HOST=${POSTGRES_HOST:-192.168.103.106}
POSTGRES_DB=${POSTGRES_DB:-dev-3}
POSTGRES_USER=${POSTGRES_USER:-dev_dopamas}
POSTGRES_PORT=${POSTGRES_PORT:-5432}

echo "📋 Database: $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
echo "👤 User: $POSTGRES_USER"
echo ""

echo "⚠️  This script will:"
echo "   1. Delete all rows from etl_run_state table (checkpoint tracking)"
echo "   2. Allow ETL to backfill data from June 2022"
echo ""

read -p "Continue? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Cancelled."
    exit 0
fi

echo ""
echo "🔄 Clearing ETL checkpoints..."

# Run psql command to clear etl_run_state
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -p "$POSTGRES_PORT" <<EOF
-- Show current state before clearing
SELECT 'Current checkpoints:' as info;
SELECT module_name, last_successful_end FROM etl_run_state;

-- Clear all checkpoints
DELETE FROM etl_run_state;

SELECT 'Checkpoints cleared. Next ETL run will backfill from June 2022.' as info;
SELECT COUNT(*) as remaining_checkpoints FROM etl_run_state;
EOF

echo ""
echo "✅ Preparation complete!"
echo ""
echo "Next steps:"
echo "1. Pull latest changes: git pull"
echo "2. Run backfill: python3 etl_master/master_etl.py"
echo "3. Monitor: tail -f etl_master/logs/*/master.log"
echo ""

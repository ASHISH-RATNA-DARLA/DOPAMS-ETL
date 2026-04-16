#!/bin/bash
################################################################################
# Quick Start - Copy & Paste Commands for Testing ETL Pipeline
# Run this from your local machine - it will SSH and execute on remote server
################################################################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          ETL Constraint Fix & Test - Quick Start              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}\n"

SERVER_HOST="eagle@192.168.103.182"
DB_HOST="192.168.103.106"
DB_USER="dev_dopamas"
DB_NAME="dev-3"
PROJECT_DIR="/data-drive/etl-process-dev"

echo -e "${YELLOW}Configuration:${NC}"
echo "  Server: $SERVER_HOST"
echo "  Database: $DB_HOST:$DB_NAME"
echo "  Project: $PROJECT_DIR"
echo ""

# Step 1: Copy test scripts to remote server
echo -e "${BLUE}Step 1: Copying test scripts to remote server...${NC}"
scp test_etl_from_config.sh "$SERVER_HOST:$PROJECT_DIR/"
scp test_each_etl.sh "$SERVER_HOST:$PROJECT_DIR/"
echo -e "${GREEN}✓ Scripts copied${NC}\n"

# Step 2: Apply database constraints
echo -e "${BLUE}Step 2: Applying database PRIMARY KEY constraints...${NC}"
ssh "$SERVER_HOST" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << 'EOSQL'
\set ON_ERROR_STOP on

-- Add PRIMARY KEY constraints for single-column upserts
ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);
ALTER TABLE public.accused ADD CONSTRAINT pk_accused_id PRIMARY KEY (accused_id);
ALTER TABLE public.persons ADD CONSTRAINT pk_persons_id PRIMARY KEY (person_id);
ALTER TABLE public.properties ADD CONSTRAINT pk_properties_id PRIMARY KEY (property_id);
ALTER TABLE public.interrogation_reports ADD CONSTRAINT pk_ir_id PRIMARY KEY (interrogation_report_id);

-- Add composite UNIQUE constraint for disposal table
ALTER TABLE public.disposal ADD CONSTRAINT uk_disposal_composite UNIQUE (crime_id, disposal_type, disposed_at);

-- Verify constraints were added
SELECT
    table_name,
    constraint_name,
    constraint_type
FROM information_schema.table_constraints
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE')
  AND table_name IN ('crimes', 'accused', 'persons', 'properties', 'interrogation_reports', 'disposal')
ORDER BY table_name;
EOSQL

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Database constraints applied${NC}\n"
else
    echo -e "${RED}✗ Failed to apply constraints${NC}"
    exit 1
fi

# Step 3: Clear ETL checkpoints
echo -e "${BLUE}Step 3: Clearing ETL checkpoints...${NC}"
ssh "$SERVER_HOST" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << 'EOSQL'
DELETE FROM etl_run_state;
SELECT 'Checkpoints cleared' as status;
EOSQL
echo -e "${GREEN}✓ Checkpoints cleared${NC}\n"

# Step 4: Make scripts executable
echo -e "${BLUE}Step 4: Making scripts executable...${NC}"
ssh "$SERVER_HOST" "cd $PROJECT_DIR && chmod +x test_etl_from_config.sh test_each_etl.sh"
echo -e "${GREEN}✓ Scripts made executable${NC}\n"

# Step 5: Run test suite
echo -e "${BLUE}Step 5: Running ETL test suite...${NC}"
echo -e "${YELLOW}Note: This will take 20-60 minutes depending on API response times${NC}\n"

read -p "Ready to start tests? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ssh "$SERVER_HOST" "cd $PROJECT_DIR && ./test_etl_from_config.sh"
    TEST_EXIT=$?
else
    echo -e "${YELLOW}Test skipped${NC}"
    exit 0
fi

echo ""

# Step 6: Retrieve and display summary
if [ $TEST_EXIT -eq 0 ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                   ✓ All Tests Passed!                         ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}\n"

    echo -e "${BLUE}Step 6: Verifying data in database...${NC}"
    ssh "$SERVER_HOST" psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" << 'EOSQL'
    SELECT 'Crimes' as table_name, COUNT(*) as record_count FROM crimes
    UNION ALL
    SELECT 'Accused', COUNT(*) FROM accused
    UNION ALL
    SELECT 'Persons', COUNT(*) FROM persons
    UNION ALL
    SELECT 'Properties', COUNT(*) FROM properties
    UNION ALL
    SELECT 'Interrogation Reports', COUNT(*) FROM interrogation_reports
    UNION ALL
    SELECT 'Disposal', COUNT(*) FROM disposal
    UNION ALL
    SELECT 'Brief Facts AI', COUNT(*) FROM brief_facts_ai
    UNION ALL
    SELECT 'Arrests', COUNT(*) FROM arrests
    ORDER BY record_count DESC;
EOSQL

    echo ""
    echo -e "${GREEN}✓ Data verification complete${NC}\n"

    echo -e "${BLUE}Next Steps:${NC}"
    echo "1. Review logs on server: $PROJECT_DIR/test_logs_*/"
    echo "2. Copy logs locally if needed:"
    echo "   scp -r $SERVER_HOST:$PROJECT_DIR/test_logs_* ."
    echo "3. Check individual ETL logs:"
    echo "   cat etl-*/etl_test_*.log"
    echo "4. Schedule daily backups or run production ETL:"
    echo "   ssh $SERVER_HOST 'cd $PROJECT_DIR && python3 etl_master/master_etl.py'"
    echo ""

else
    echo -e "${RED}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║                  ✗ Tests Failed                              ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════════╝${NC}\n"

    echo -e "${BLUE}Troubleshooting:${NC}"
    echo "1. Check constraint application:"
    echo "   ssh $SERVER_HOST 'psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c \"SELECT * FROM information_schema.table_constraints WHERE constraint_type=\\'PRIMARY KEY\\';\"'"
    echo ""
    echo "2. View detailed logs:"
    echo "   ssh $SERVER_HOST 'tail -100 $PROJECT_DIR/test_logs_*/crimes_test_*.log'"
    echo ""
    echo "3. Check database state:"
    echo "   ssh $SERVER_HOST 'psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c \"SELECT COUNT(*) FROM crimes;\"'"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Quick start complete!${NC}"
exit 0

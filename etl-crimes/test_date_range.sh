#!/bin/bash
# Test Script: Compare API response vs ETL results
# Date Range: 2025-10-01 to 2025-10-02

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
elif [ -f "../.env" ]; then
    export $(grep -v '^#' ../.env | xargs)
fi

# Validate required env vars
if [ -z "$DOPAMAS_API_URL" ] || [ -z "$DOPAMAS_API_KEY" ]; then
    echo "❌ ERROR: DOPAMAS_API_URL and DOPAMAS_API_KEY must be set in .env"
    exit 1
fi

POSTGRES_USER=${POSTGRES_USER:-${DB_USER:-postgres}}
POSTGRES_DB=${POSTGRES_DB:-${DB_NAME:-}}

echo "================================================================================"
echo "🧪 DOPAMAS ETL - Test Date Range Verification"
echo "================================================================================"
echo ""

# Test dates
FROM_DATE="2025-10-01"
TO_DATE="2025-10-02"

echo "📅 Test Date Range: $FROM_DATE to $TO_DATE"
echo ""

# Step 1: Get API response count
echo "================================================================================"
echo "Step 1: Fetching from API..."
echo "================================================================================"
echo ""

API_RESPONSE=$(curl -s -X GET \
  "${DOPAMAS_API_URL}/crimes?fromDate=$FROM_DATE&toDate=$TO_DATE" \
  -H "x-api-key: ${DOPAMAS_API_KEY}")

echo "$API_RESPONSE" > /tmp/api_response_test.json

# Count crimes from API
if echo "$API_RESPONSE" | jq . > /dev/null 2>&1; then
    API_COUNT=$(echo "$API_RESPONSE" | jq -r '.data | if type=="array" then length else 1 end')
    echo "✅ API Response received"
    echo "📊 API Crime Count: $API_COUNT"
    
    # Show first crime ID
    if [ "$API_COUNT" -gt 0 ]; then
        FIRST_CRIME=$(echo "$API_RESPONSE" | jq -r '.data | if type=="array" then .[0].CRIME_ID else .CRIME_ID end')
        echo "🔍 First Crime ID: $FIRST_CRIME"
    fi
else
    echo "❌ Invalid JSON response from API"
    API_COUNT="ERROR"
fi

echo ""

# Step 2: Run ETL
echo "================================================================================"
echo "Step 2: Running ETL..."
echo "================================================================================"
echo ""

python3 etl_crimes.py

echo ""

# Step 3: Query database
echo "================================================================================"
echo "Step 3: Checking Database..."
echo "================================================================================"
echo ""

DB_COUNT=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "
    SELECT COUNT(*) 
    FROM crimes 
    WHERE fir_date BETWEEN '$FROM_DATE' AND '$TO_DATE'
")

DB_COUNT=$(echo $DB_COUNT | xargs)  # Trim whitespace

echo "📊 Database Crime Count: $DB_COUNT"

# Show sample from database
echo ""
echo "Sample crimes from database:"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT crime_id, fir_num, crime_type, fir_date::date
    FROM crimes 
    WHERE fir_date BETWEEN '$FROM_DATE' AND '$TO_DATE'
    LIMIT 3
"

echo ""

# Step 4: Compare
echo "================================================================================"
echo "📊 COMPARISON RESULTS"
echo "================================================================================"
echo ""
echo "API Response Count:     $API_COUNT"
echo "Database Count:         $DB_COUNT"
echo ""

if [ "$API_COUNT" = "$DB_COUNT" ]; then
    echo "✅ SUCCESS: Counts match perfectly!"
    echo ""
    echo "✅ ETL is working correctly for this date range"
    echo "✅ Ready to process full date range (2022-10-01 to 2025-10-15)"
else
    echo "⚠️  WARNING: Counts don't match"
    echo ""
    echo "Possible reasons:"
    echo "  - API returned data but ETL had errors (check logs)"
    echo "  - Some crimes were skipped (invalid PS_CODE)"
    echo "  - Date filtering difference"
fi

echo ""
echo "================================================================================"
echo ""

# Show detailed comparison
echo "View full API response:"
echo "  cat /tmp/api_response_test.json | jq ."
echo ""
echo "View ETL logs for details about what was processed"
echo ""



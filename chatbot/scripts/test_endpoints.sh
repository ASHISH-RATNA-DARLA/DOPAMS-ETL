#!/bin/bash
##############################################################################
# API Endpoints Testing Script
# Tests all Flask API endpoints to ensure they're working correctly
##############################################################################

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BASE_URL="${BASE_URL:-http://localhost:5000}"
SESSION_ID="test-session-$(date +%s)"

# Counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

##############################################################################
# Helper Functions
##############################################################################

print_header() {
    echo -e "\n${BLUE}============================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================================${NC}\n"
}

print_test() {
    echo -e "${YELLOW}Testing:${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅ PASS${NC} - $1\n"
    ((PASSED_TESTS++))
    ((TOTAL_TESTS++))
}

print_failure() {
    echo -e "${RED}❌ FAIL${NC} - $1"
    echo -e "${RED}Response:${NC} $2\n"
    ((FAILED_TESTS++))
    ((TOTAL_TESTS++))
}

test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local description=$4
    
    print_test "$description"
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$BASE_URL$endpoint")
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$BASE_URL$endpoint")
    fi
    
    # Extract HTTP code (last line)
    http_code=$(echo "$response" | tail -n 1)
    # Extract body (everything except last line)
    body=$(echo "$response" | sed '$d')
    
    # Check HTTP code
    if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
        print_success "$description (HTTP $http_code)"
        echo "Response: $body" | head -n 5
        return 0
    else
        print_failure "$description (HTTP $http_code)" "$body"
        return 1
    fi
}

##############################################################################
# Main Tests
##############################################################################

print_header "🚀 API ENDPOINTS TESTING"

echo -e "Testing URL: ${GREEN}$BASE_URL${NC}"
echo -e "Session ID: ${GREEN}$SESSION_ID${NC}\n"

# Check if server is running
print_test "Checking if server is accessible"
if curl -s --max-time 5 "$BASE_URL" > /dev/null; then
    print_success "Server is accessible"
else
    echo -e "${RED}❌ ERROR: Server is not accessible at $BASE_URL${NC}"
    echo -e "${YELLOW}Make sure the Flask application is running:${NC}"
    echo -e "  python app.py"
    exit 1
fi

##############################################################################
# Test 1: Health Check
##############################################################################

print_header "Test 1: Health Check Endpoint"

test_endpoint "GET" "/api/health" "" "GET /api/health"

##############################################################################
# Test 2: Get Schema
##############################################################################

print_header "Test 2: Get Database Schema"

test_endpoint "GET" "/api/schema" "" "GET /api/schema"

##############################################################################
# Test 3: Chat - Simple Query
##############################################################################

print_header "Test 3: Chat - Simple Query"

chat_data='{
  "message": "Show me all tables",
  "session_id": "'$SESSION_ID'"
}'

test_endpoint "POST" "/api/chat" "$chat_data" "POST /api/chat (simple query)"

##############################################################################
# Test 4: Chat - Count Query
##############################################################################

print_header "Test 4: Chat - Count Query"

chat_data='{
  "message": "Count total records",
  "session_id": "'$SESSION_ID'"
}'

test_endpoint "POST" "/api/chat" "$chat_data" "POST /api/chat (count query)"

##############################################################################
# Test 5: Get Conversation History
##############################################################################

print_header "Test 5: Get Conversation History"

test_endpoint "GET" "/api/chat/history/$SESSION_ID" "" "GET /api/chat/history/<session_id>"

##############################################################################
# Test 6: Query Validation - Valid SQL
##############################################################################

print_header "Test 6: Query Validation - Valid SQL"

validate_data='{
  "query": "SELECT * FROM users LIMIT 10",
  "type": "sql"
}'

test_endpoint "POST" "/api/query/validate" "$validate_data" "POST /api/query/validate (valid SQL)"

##############################################################################
# Test 7: Query Validation - Invalid SQL (Should Fail)
##############################################################################

print_header "Test 7: Query Validation - Dangerous SQL"

validate_data='{
  "query": "DROP TABLE users",
  "type": "sql"
}'

print_test "POST /api/query/validate (dangerous SQL - should be rejected)"

response=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d "$validate_data" \
    "$BASE_URL/api/query/validate")

http_code=$(echo "$response" | tail -n 1)
body=$(echo "$response" | sed '$d')

# Should succeed (200) but query should be invalid
if [ "$http_code" -eq 200 ]; then
    # Check if query was marked as invalid
    if echo "$body" | grep -q '"valid".*false'; then
        print_success "Dangerous SQL correctly rejected"
        echo "Response: $body" | head -n 3
    else
        print_failure "Dangerous SQL was NOT rejected!" "$body"
    fi
else
    print_failure "Validation endpoint error (HTTP $http_code)" "$body"
fi

##############################################################################
# Test 8: Chat - Rate Limiting Test (Optional)
##############################################################################

print_header "Test 8: Rate Limiting (Optional)"

echo -e "${YELLOW}Testing rate limiting by sending multiple requests...${NC}"

rate_limit_failures=0
for i in {1..5}; do
    response=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -d '{"message":"test","session_id":"rate-test"}' \
        "$BASE_URL/api/chat")
    
    if [ "$response" -eq 429 ]; then
        ((rate_limit_failures++))
    fi
    sleep 0.1
done

if [ $rate_limit_failures -gt 0 ]; then
    print_success "Rate limiting is working (got $rate_limit_failures 429 responses)"
else
    echo -e "${YELLOW}⚠️  INFO${NC} - Rate limiting not triggered (this is OK for low request volume)\n"
    ((TOTAL_TESTS++))
fi

##############################################################################
# Test 9: Clear Conversation History
##############################################################################

print_header "Test 9: Clear Conversation History"

test_endpoint "DELETE" "/api/chat/history/$SESSION_ID" "" "DELETE /api/chat/history/<session_id>"

##############################################################################
# Test 10: Invalid Endpoint (404)
##############################################################################

print_header "Test 10: Invalid Endpoint"

print_test "GET /api/invalid (should return 404)"

response=$(curl -s -w "\n%{http_code}" "$BASE_URL/api/invalid")
http_code=$(echo "$response" | tail -n 1)

if [ "$http_code" -eq 404 ]; then
    print_success "Correctly returns 404 for invalid endpoint"
else
    print_failure "Expected 404, got HTTP $http_code" "$(echo "$response" | sed '$d')"
fi

##############################################################################
# Summary
##############################################################################

print_header "📊 TEST SUMMARY"

echo -e "Total Tests:  ${BLUE}$TOTAL_TESTS${NC}"
echo -e "✅ Passed:    ${GREEN}$PASSED_TESTS${NC}"
echo -e "❌ Failed:    ${RED}$FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}🎉🎉🎉 ALL TESTS PASSED! 🎉🎉🎉${NC}"
    echo -e "${GREEN}Your API is working correctly!${NC}\n"
    exit 0
else
    echo -e "\n${RED}❌ SOME TESTS FAILED!${NC}"
    echo -e "${YELLOW}Please review the failures above and check:${NC}"
    echo -e "  1. Database connections"
    echo -e "  2. LLM service (Ollama)"
    echo -e "  3. Application logs: tail -f chatbot.log\n"
    exit 1
fi


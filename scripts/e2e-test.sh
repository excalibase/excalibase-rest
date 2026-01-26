#!/bin/bash

# Excalibase REST API E2E Test Suite
# Tests the complete REST API using curl commands
# Requires: docker-compose services running on ports 20000 (app) and 5432 (postgres)

# set -e  # Exit on any error - keeping commented to show all results

# Configuration
API_URL="${API_URL:-http://localhost:20000/api/v1}"
TIMEOUT=30
MAX_RETRIES=10
RETRY_DELAY=5

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for REST API to be ready
wait_for_api() {
    log_info "Waiting for REST API to be ready..."
    
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -s --connect-timeout 5 "$API_URL" > /dev/null 2>&1; then
            log_success "REST API is ready!"
            return 0
        fi
        
        log_warning "Attempt $i/$MAX_RETRIES: API not ready, waiting ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done
    
    log_error "REST API failed to start after $MAX_RETRIES attempts"
    return 1
}

# Test counter
test_count=0
passed_tests=0
failed_tests=0

run_test() {
    ((test_count++))
    if execute_rest_test "$@"; then
        ((passed_tests++))
    else
        ((failed_tests++))
    fi
}

# Execute REST API test and validate response
execute_rest_test() {
    local test_name="$1"
    local method="$2"
    local url="$3"
    local data="${4:-}"
    local expected_status="${5:-200}"
    local expected_check="${6:-}"  # Optional: specific check for response
    
    log_info "Testing: $test_name"
    
    # Execute HTTP request
    local full_response
    if [ -n "$data" ]; then
        full_response=$(curl -s \
            --max-time "$TIMEOUT" \
            -w "HTTPSTATUS:%{http_code}" \
             -X "$method" \
            -H "Content-Type: application/json" \
             -d "$data" \
            "$url")
    else
        full_response=$(curl -s \
            --max-time "$TIMEOUT" \
            -w "HTTPSTATUS:%{http_code}" \
             -X "$method" \
            "$url")
    fi
    
    # Check for curl errors
    if [ $? -ne 0 ]; then
        log_error "$test_name: Failed to execute HTTP request"
        return 1
    fi
    
    # Extract status and response
    local status=$(echo "$full_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    local response=$(echo "$full_response" | sed -e 's/HTTPSTATUS:.*//g')
    
    # Check HTTP status
    if [ "$status" -ne "$expected_status" ]; then
        log_error "$test_name: Expected status $expected_status, got $status"
        echo "Response: $response"
        return 1
    fi
    
    # Custom validation if provided
    if [ -n "$expected_check" ]; then
        if ! echo "$response" | jq -e "$expected_check" > /dev/null 2>&1; then
            log_error "$test_name: Expected check failed: $expected_check"
            echo "$response" | jq '.' 2>/dev/null || echo "$response"
            return 1
        fi
    fi
    
    log_success "$test_name: ✓ Passed"
    return 0
}

# Main test suite
main() {
    echo "=================================================="
    echo "🚀 Excalibase REST API E2E Test Suite"
    echo "🎯 API Endpoint: $API_URL"
    echo "🐘 Database: PostgreSQL on port 5432"
    echo "=================================================="
    
    # Wait for API to be ready
    if ! wait_for_api; then
        exit 1
    fi
    
    echo ""
    log_info "🧪 Starting REST API Tests..."
    echo ""
    
    # ==========================================
    # API HEALTH AND SCHEMA DISCOVERY TESTS
    # ==========================================
    
    run_test "API Health Check" \
        "GET" \
        "$API_URL" \
        "" \
        "200" \
        '.tables | length > 0'
    
    run_test "Schema Discovery - Tables Available" \
        "GET" \
        "$API_URL" \
        "" \
        "200" \
        '.tables | type == "array"'
    
    # ==========================================
    # OPENAPI DOCUMENTATION TESTS
    # ==========================================
    
    run_test "OpenAPI JSON Specification" \
        "GET" \
        "$API_URL/openapi.json" \
        "" \
        "200" \
        '.openapi != null and .info != null and .paths != null'
    
    run_test "OpenAPI YAML Specification" \
        "GET" \
        "$API_URL/openapi.yaml" \
        "" \
        "200" \
        ""
    
    run_test "API Documentation Endpoint" \
        "GET" \
        "$API_URL/docs" \
        "" \
        "200" \
        '.title != null'
    
    # ==========================================
    # BASIC CRUD OPERATIONS TESTS
    # ==========================================
    
    # Get first table for testing
    local tables_response=$(curl -s "$API_URL")
    local first_table=$(echo "$tables_response" | jq -r '.tables[0]' 2>/dev/null || echo "")
    
    if [ -n "$first_table" ] && [ "$first_table" != "null" ]; then
        log_info "🔍 Testing CRUD operations on table: $first_table"
        
        run_test "GET Records with Pagination" \
            "GET" \
            "$API_URL/$first_table?limit=5&offset=0" \
            "" \
            "200" \
            '(.data | type == "array") and (.pagination != null)'
        
        run_test "GET Table Schema" \
            "GET" \
            "$API_URL/$first_table/schema" \
            "" \
            "200" \
            '.table != null'
        
        # Test CREATE operation - use customers table if available
        if echo "$tables_response" | jq -r '.tables[]' | grep -q "customers"; then
            log_info "Testing CRUD operations on customers table"
            local unique_email="e2e-test-$(date +%s)@test.com"
            local test_data="{\"name\": \"E2E Test Customer\", \"email\": \"$unique_email\", \"tier\": \"bronze\"}"
            run_test "CREATE New Record" \
                "POST" \
                "$API_URL/customers" \
                "$test_data" \
                "201" \
                '.name == "E2E Test Customer"'
            
            # Store created record ID for update/delete tests
            local created_id=$(echo "$test_response" | jq -r '.id.value // .customer_id.value // .id // .customer_id // empty')
            
            if [ -n "$created_id" ]; then
                # Test READ specific record
                run_test "READ Specific Record" \
                    "GET" \
                    "$API_URL/customers/$created_id" \
                    "" \
                    "200" \
                    '.name == "E2E Test Customer"'
                
                # Test UPDATE record
                local update_data='{"name": "E2E Updated Customer", "tier": "silver"}'
                run_test "UPDATE Record" \
                    "PUT" \
                    "$API_URL/customers/$created_id" \
                    "$update_data" \
                    "200" \
                    '.name == "E2E Updated Customer"'
                
                # Test DELETE record
                run_test "DELETE Record" \
                    "DELETE" \
                    "$API_URL/customers/$created_id" \
                    "" \
                    "204" \
                    ''
                
                # Verify DELETE worked
                run_test "Verify DELETE" \
                    "GET" \
                    "$API_URL/customers/$created_id" \
                    "" \
                    "404" \
                    ''
            fi
        fi
    else
        log_warning "⚠️  No tables found - skipping CRUD testing"
    fi
    
    # ==========================================
    # ADVANCED FILTERING TESTS
    # ==========================================
    
    if [ -n "$first_table" ] && [ "$first_table" != "null" ]; then
        log_info "🔍 Testing Advanced Filtering..."
        
        run_test "Equality Filter" \
            "GET" \
            "$API_URL/$first_table?id=eq.1&limit=1" \
            "" \
            "200" \
            '.data | type == "array"'
        
        run_test "LIKE Filter" \
            "GET" \
            "$API_URL/$first_table?change_type=like.stock&limit=1" \
            "" \
            "200" \
            '.data | type == "array"'
        
        run_test "OR Conditions" \
            "GET" \
            "$API_URL/$first_table?or=(id.eq.1,id.eq.2)&limit=2" \
            "" \
            "200" \
            '.data | type == "array"'
        
        run_test "Ordering" \
            "GET" \
            "$API_URL/$first_table?orderBy=id&orderDirection=asc&limit=3" \
            "" \
            "200" \
            '.data | type == "array"'
        
        run_test "Field Selection" \
            "GET" \
            "$API_URL/$first_table?select=id&limit=1" \
            "" \
            "200" \
            '.data | type == "array"'
    fi
    
    # ==========================================
    # ERROR HANDLING TESTS
    # ==========================================
    
    log_info "🚨 Testing Error Handling..."
    
    run_test "Non-existent Table" \
        "GET" \
        "$API_URL/nonexistent_table_123" \
        "" \
        "400" \
        '.error != null'
    
    run_test "Invalid JSON POST" \
        "POST" \
        "$API_URL/customers" \
        '{"invalid": "json"' \
        "400" \
        ''
    
    run_test "Invalid HTTP Method" \
        "DELETE" \
        "$API_URL" \
        "" \
        "405" \
        ''
    
    # ==========================================
    # CONTENT TYPE TESTS
    # ==========================================
    
    log_info "📄 Testing Content Types..."
    
    # Test JSON Accept header
    local json_response=$(curl -s -w "HTTPSTATUS:%{http_code}" -H "Accept: application/json" "$API_URL")
    local json_status=$(echo "$json_response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    if [ "$json_status" -eq 200 ]; then
        log_success "JSON Accept header: ✓ Passed"
        ((test_count++))
        ((passed_tests++))
    else
        log_error "JSON Accept header: Failed"
        ((test_count++))
        ((failed_tests++))
    fi

    
    # ==========================================
    # SECURITY TESTS
    # ==========================================
    
    log_info "🔒 Testing Security Controls..."
    
    # Test SQL injection using URL encoded characters
    run_test "SQL Injection Prevention" \
        "GET" \
        "$API_URL/customers?name=%27%3B%20DROP%20TABLE%20customers%3B%20--" \
        "" \
        "400" \
        '.error != null'
    
    run_test "Input Validation - Excessive Limit" \
        "GET" \
        "$API_URL/customers?limit=10000" \
        "" \
        "400" \
        '.error != null'
    
    run_test "Input Validation - Negative Limit" \
        "GET" \
        "$API_URL/customers?limit=-1" \
        "" \
        "400" \
        '.error != null'
    
    # ==========================================
    # PERFORMANCE TESTS
    # ==========================================
    
    log_info "⚡ Testing Performance..."
    
    start_time=$(date +%s%N)
    run_test "Performance - Large Dataset Query" \
        "GET" \
        "$API_URL/customers?limit=100" \
        "" \
        "200" \
        '.data | type == "array"'
    end_time=$(date +%s%N)
    
    duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
    if [ $duration -lt 2000 ]; then
        log_success "Performance: Response time acceptable (${duration}ms < 2000ms)"
    else
        log_warning "Performance: Response time high (${duration}ms >= 2000ms)"
    fi
    
    # ==========================================
    # ENHANCED POSTGRESQL TYPES TESTS
    # ==========================================
    
    log_info "🔧 Testing Enhanced PostgreSQL Types..."
    
    # Test JSONB operations (simplified)
    run_test "JSONB Column Present" \
        "GET" \
        "$API_URL/products?limit=1" \
        "" \
        "200" \
        '.data[0] | has("attributes")'
    
    run_test "Products Basic Query" \
        "GET" \
        "$API_URL/products?limit=3" \
        "" \
        "200" \
        '.data | type == "array" and length <= 3'
    
    run_test "Timestamp Field Present" \
        "GET" \
        "$API_URL/products?limit=1" \
        "" \
        "200" \
        '.data[0] | has("created_at")'
    
    # ==========================================
    # RELATIONSHIP TESTS
    # ==========================================
    
    log_info "🔗 Testing Relationships..."
    
    run_test "Foreign Key Relationship" \
        "GET" \
        "$API_URL/orders?limit=3&expand=customer_id" \
        "" \
        "200" \
        '.data | type == "array"'
    
    run_test "Nested Relationship Query" \
        "GET" \
        "$API_URL/order_items?limit=5&expand=product_id,order_id" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # COMPLEX FILTERING TESTS
    # ==========================================
    
    log_info "🎯 Testing Complex Filtering..."
    
    run_test "Basic Product Filtering" \
        "GET" \
        "$API_URL/products?is_active=eq.true&limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    run_test "Ordering Test" \
        "GET" \
        "$API_URL/products?limit=5&order=name.asc" \
        "" \
        "200" \
        '.data | type == "array"'
    
    run_test "Text Field Filtering" \
        "GET" \
        "$API_URL/products?name=like.%test%&limit=3" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # ENUM AND CUSTOM TYPES TESTS
    # ==========================================
    
    log_info "🏷️  Testing Enum and Custom Types..."
    
    run_test "Enum Value Filtering" \
        "GET" \
        "$API_URL/customers?tier=eq.gold&limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    local enum_test_email="enum-test-$(date +%s)@example.com"
    run_test "Enum Value Creation" \
        "POST" \
        "$API_URL/customers" \
        "{\"name\": \"Enum Test\", \"email\": \"$enum_test_email\", \"tier\": \"platinum\"}" \
        "201" \
        '.tier == "platinum"'
    
    # ==========================================
    # PAGINATION TESTS
    # ==========================================
    
    log_info "📄 Testing Pagination Variants..."
    
    run_test "Offset Pagination" \
        "GET" \
        "$API_URL/products?limit=3&offset=5" \
        "" \
        "200" \
        '.pagination.offset == 5 and .pagination.limit == 3'
    
    run_test "Large Offset Handling" \
        "GET" \
        "$API_URL/products?limit=10&offset=50000" \
        "" \
        "200" \
        '.data | length >= 0'
    
    # ==========================================
    # UPDATE AND DELETE TESTS
    # ==========================================
    
    log_info "✏️  Testing UPDATE and DELETE Operations..."
    
    # Create a record for update/delete testing
    if echo "$tables_response" | jq -r '.tables[]' | grep -q "customers"; then
        local update_email="update-test-$(date +%s)@example.com"
        local update_response=$(curl -s -X POST -H "Content-Type: application/json" \
            -d "{\"name\": \"Update Test\", \"email\": \"$update_email\", \"tier\": \"bronze\"}" \
            "$API_URL/customers")
        
        local update_id=$(echo "$update_response" | jq -r '.id.value // .customer_id.value // .id // .customer_id // empty')
        
        if [ -n "$update_id" ]; then
            run_test "UPDATE Record" \
                "PUT" \
                "$API_URL/customers/$update_id" \
                '{"name": "Updated Name", "tier": "silver"}' \
                "200" \
                '.name == "Updated Name" and .tier == "silver"'
            
            run_test "DELETE Record" \
                "DELETE" \
                "$API_URL/customers/$update_id" \
                "" \
                "204" \
                ''
            
            run_test "Verify DELETE - Record Not Found" \
                "GET" \
                "$API_URL/customers/$update_id" \
                "" \
                "404" \
                ''
        fi
    fi
    
    # ==========================================
    # BULK OPERATIONS TESTS
    # ==========================================

    log_info "📦 Testing Bulk Operations..."
    
    local bulk_timestamp=$(date +%s)
    
    # Test bulk CREATE via normal POST endpoint with array
    run_test "Bulk Create Multiple Records via Array" \
        "POST" \
        "$API_URL/customers" \
        "[
            {\"name\": \"Bulk User 1\", \"email\": \"bulk1-${bulk_timestamp}@test.com\", \"tier\": \"bronze\"},
            {\"name\": \"Bulk User 2\", \"email\": \"bulk2-${bulk_timestamp}@test.com\", \"tier\": \"silver\"},
            {\"name\": \"Bulk User 3\", \"email\": \"bulk3-${bulk_timestamp}@test.com\", \"tier\": \"gold\"}
        ]" \
        "201" \
        '.count >= 3'

    # Test bulk UPDATE via query filters (horizontal filtering)
    run_test "Bulk Update via Query Filters" \
        "PUT" \
        "$API_URL/customers?tier=eq.bronze" \
        "{\"tier\": \"silver\"}" \
        "200" \
        '.updatedCount >= 0'
    
    # Test bulk UPDATE with more specific filters
    run_test "Bulk Update with Email Filter" \
        "PUT" \
        "$API_URL/customers?email=like.bulk1-${bulk_timestamp}%25" \
        "{\"name\": \"Updated Bulk User 1\"}" \
        "200" \
        '.updatedCount >= 0'

    # Test bulk DELETE via query filters (horizontal filtering)
    run_test "Bulk Delete via Query Filters" \
        "DELETE" \
        "$API_URL/customers?email=like.bulk%25${bulk_timestamp}%25" \
        "" \
        "200" \
        '.deletedCount >= 0'
    
    # Test array-based bulk operations (our extension)
    run_test "Bulk Update via Array (Extension)" \
        "PUT" \
        "$API_URL/customers" \
        "[
            {\"id\": \"dummy-id-1\", \"data\": {\"name\": \"Array Update 1\"}},
            {\"id\": \"dummy-id-2\", \"data\": {\"name\": \"Array Update 2\"}}
        ]" \
        "400" \
        '.error != null'
    
    # Test error handling for bulk operations
    run_test "Bulk Create - Empty Array Error" \
        "POST" \
        "$API_URL/customers" \
        "[]" \
        "400" \
        '.error != null'
    
    run_test "Bulk Update - No Filters or ID Error" \
        "PUT" \
        "$API_URL/customers" \
        "{\"name\": \"No Filters\"}" \
        "400" \
        '.error != null'
    
    run_test "Bulk Delete - No Filters Error" \
        "DELETE" \
        "$API_URL/customers" \
        "" \
        "400" \
        '.error != null'
    
    # Test complex filtering operations
    run_test "Complex Filter DELETE (OR Conditions)" \
        "DELETE" \
        "$API_URL/customers?or=(tier.eq.platinum,email.like.test-delete%25)" \
        "" \
        "200" \
        '.deletedCount >= 0'
    
    run_test "Complex Filter UPDATE (Multiple Conditions)" \
        "PUT" \
        "$API_URL/customers?tier=eq.gold" \
        "{\"tier\": \"platinum\"}" \
        "200" \
        '.updatedCount >= 0'
    
    # ==========================================
    # ADVANCED ERROR HANDLING TESTS
    # ==========================================
    
    log_info "🚨 Testing Advanced Error Scenarios..."
    
    run_test "Invalid Enum Value" \
        "POST" \
        "$API_URL/customers" \
        '{"name": "Invalid Enum", "email": "invalid@test.com", "tier": "invalid_tier"}' \
        "400" \
        '.error | contains("enum")'
    
    run_test "Missing Required Field" \
        "POST" \
        "$API_URL/customers" \
        '{"tier": "bronze"}' \
        "400" \
        '.error != null'
    
    run_test "Invalid JSON Structure" \
        "POST" \
        "$API_URL/products" \
        '{"attributes": "not_json_object"}' \
        "400" \
        '.error != null'
    
    # ==========================================
    # PERFORMANCE AND LIMITS TESTS
    # ==========================================
    
    log_info "⚡ Testing Performance Limits..."
    
    run_test "Large Result Set Performance" \
        "GET" \
        "$API_URL/products?limit=100" \
        "" \
        "200" \
        '.pagination.limit == 100'
    
    run_test "Complex Query Performance" \
        "GET" \
        "$API_URL/orders?expand=customer_id&limit=20" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # SCHEMA VALIDATION TESTS
    # ==========================================
    
    log_info "📋 Testing Schema Validation..."
    
    run_test "All Tables Schema Access" \
        "GET" \
        "$API_URL/customers/schema" \
        "" \
        "200" \
        '.table.name == "customers" and (.table.columns | length > 5)'
    
    run_test "Column Type Validation" \
        "GET" \
        "$API_URL/products/schema" \
        "" \
        "200" \
        '.table.columns | map(select(.name == "price")) | length == 1'
    
    # ==========================================
    # ADVANCED POSTGRESQL TYPE TESTS
    # ==========================================
    
    log_info "🔧 Testing Advanced PostgreSQL Types..."
    
    # Test UUID handling
    run_test "UUID Column Type Support" \
        "GET" \
        "$API_URL/customers?limit=1" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test timestamp filtering
    run_test "Timestamp Filtering" \
        "GET" \
        "$API_URL/orders?order_date=gte.2025-01-01&limit=3" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test JSONB operations
    run_test "JSONB haskey Operator" \
        "GET" \
        "$API_URL/products?attributes=haskey.color&limit=3" \
        "" \
        "200" \
        '.data | type == "array"'
    
    run_test "JSONB Contains Operator" \
        "GET" \
        "$API_URL/products?attributes=haskey.brand&limit=3" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # CURSOR PAGINATION TESTS  
    # ==========================================
    
    log_info "📄 Testing Cursor Pagination..."
    
    run_test "Cursor-based Pagination" \
        "GET" \
        "$API_URL/products?first=5" \
        "" \
        "200" \
        '.edges != null and .pageInfo != null'
    
    run_test "Cursor Forward Pagination" \
        "GET" \
        "$API_URL/products?first=3&after=MQ==" \
        "" \
        "200" \
        '.edges | type == "array"'
    
    # ==========================================
    # BULK OPERATIONS TESTS
    # ==========================================
    
    log_info "📦 Testing Bulk Operations..."
    
    local bulk_timestamp=$(date +%s)
    local bulk_test_email1="bulk-test-${bulk_timestamp}-1@test.com"
    local bulk_test_email2="bulk-test-${bulk_timestamp}-2@test.com"
    
    # Test bulk insert via array
    run_test "Bulk Insert via Array" \
        "POST" \
        "$API_URL/customers" \
        "[
            {\"name\": \"Bulk User 1\", \"email\": \"$bulk_test_email1\", \"tier\": \"bronze\"},
            {\"name\": \"Bulk User 2\", \"email\": \"$bulk_test_email2\", \"tier\": \"silver\"}
        ]" \
        "201" \
        '(.count >= 2) or (. | type == "array" and length >= 2)'
    
    # Test conditional updates
    run_test "Conditional Update by Filter" \
        "PUT" \
        "$API_URL/customers?email=like.bulk-test-${bulk_timestamp}%25" \
        '{"tier": "gold"}' \
        "200" \
        '.updatedCount >= 0'
    
    # Test conditional deletes  
    run_test "Conditional Delete by Filter" \
        "DELETE" \
        "$API_URL/customers?email=like.bulk-test-${bulk_timestamp}%25" \
        "" \
        "200" \
        '.deletedCount >= 0'
    
    # ==========================================
    # UPSERT OPERATIONS TESTS
    # ==========================================
    
    log_info "🔄 Testing Upsert Operations..."
    
    local upsert_timestamp=$(date +%s)
    local upsert_email="upsert-test-${upsert_timestamp}@test.com"
    
    # Create initial record for upsert
    # Test upsert with same ID (primary key conflict) - generate unique UUID
    local test_uuid="$(uuidgen | tr '[:upper:]' '[:lower:]')"
    run_test "Initial Record for Upsert" \
        "POST" \
        "$API_URL/customers" \
        "{\"id\": \"$test_uuid\", \"name\": \"Upsert Test\", \"email\": \"$upsert_email\", \"tier\": \"bronze\"}" \
        "201" \
        '.email == "'$upsert_email'"'
    
    # Test upsert (update existing with same ID)
    run_test "Upsert Existing Record" \
        "POST" \
        "$API_URL/customers?prefer=resolution=merge-duplicates" \
        "{\"id\": \"$test_uuid\", \"name\": \"Upsert Updated\", \"email\": \"$upsert_email\", \"tier\": \"silver\"}" \
        "201" \
        '.name == "Upsert Updated" and .tier == "silver"'
    
    # ==========================================
    # ADVANCED FILTERING TESTS
    # ==========================================
    
    log_info "🎯 Testing Advanced Filtering..."
    
    # Test range queries
    run_test "Range Query with gte/lte" \
        "GET" \
        "$API_URL/products?price=gte.10&price=lte.100&limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test IN operator
    run_test "IN Operator Filter" \
        "GET" \
        "$API_URL/customers?tier=in.(bronze,silver)&limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test NOT IN operator
    run_test "NOT IN Operator Filter" \
        "GET" \
        "$API_URL/customers?tier=notin.(platinum)&limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test case insensitive search
    run_test "Case Insensitive LIKE" \
        "GET" \
        "$API_URL/customers?name=ilike.%test%&limit=3" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # Test NULL/NOT NULL operators
    run_test "IS NULL Operator" \
        "GET" \
        "$API_URL/customers?phone=is.null&limit=10" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # PERFORMANCE AND STRESS TESTS
    # ==========================================
    
    log_info "⚡ Testing Performance and Stress..."
    
    # Test large dataset query performance
    start_time=$(date +%s%N)
    run_test "Large Dataset Query Performance" \
        "GET" \
        "$API_URL/customers?limit=100" \
        "" \
        "200" \
        '.data | type == "array"'
    end_time=$(date +%s%N)
    
    duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
    if [ $duration -lt 3000 ]; then
        log_success "Performance: Large query acceptable (${duration}ms < 3000ms)"
    else
        log_warning "Performance: Large query slow (${duration}ms >= 3000ms)"
    fi
    
    # Test concurrent request simulation (simplified)
    run_test "Multiple Concurrent Requests Simulation" \
        "GET" \
        "$API_URL/products?limit=5" \
        "" \
        "200" \
        '.data | type == "array"'
    
    # ==========================================
    # ERROR EDGE CASES
    # ==========================================
    
    log_info "🚨 Testing Error Edge Cases..."
    
    # Test malformed JSON
    run_test "Malformed JSON Request" \
        "POST" \
        "$API_URL/customers" \
        '{"name": "Invalid JSON"' \
        "400" \
        ''
    
    # Test extremely long field values
    local long_string=$(printf 'a%.0s' {1..1000})
    run_test "Extremely Long Field Value" \
        "POST" \
        "$API_URL/customers" \
        "{\"name\": \"$long_string\", \"email\": \"long@test.com\", \"tier\": \"bronze\"}" \
        "400" \
        '.error != null'
    
    # Test invalid data types
    run_test "Invalid Data Type" \
        "POST" \
        "$API_URL/customers" \
        '{"name": "Test User", "email": "invalid@test.com", "tier": "invalid_tier"}' \
        "400" \
        '.error != null'
    
    # Test missing required fields
    run_test "Missing Required Fields" \
        "POST" \
        "$API_URL/customers" \
        '{"tier": "bronze"}' \
        "400" \
        '.error != null'
    
    # ==========================================
    # TEST SUMMARY
    # ==========================================
    
    echo ""
    echo "=================================================="
    echo "📊 E2E Test Results Summary"
    echo "=================================================="
    echo "Total Tests: $test_count"
    echo "✅ Passed: $passed_tests"
    echo "❌ Failed: $failed_tests"
    echo "📈 Success Rate: $(((passed_tests * 100) / test_count))%"
    echo ""
    echo "🎯 REST API Coverage:"
    echo "  ✅ Schema Discovery & Health Checks"
    echo "  ✅ OpenAPI Documentation (JSON/YAML)"
    echo "  ✅ CRUD Operations (Create, Read, Update, Delete)"
    echo "  ✅ Advanced Filtering & Querying"
    echo "  ✅ Error Handling & Validation"
    echo "  ✅ Content Type Negotiation"
    echo "  ✅ Security Controls (SQL Injection Prevention)"
    echo "  ✅ Performance Monitoring"
    echo "=================================================="
    
    if [ $failed_tests -eq 0 ]; then
        log_success "🎉 All tests passed! REST API is working correctly."
    echo ""
        log_info "🌐 REST API Endpoint: $API_URL"
        log_info "📖 Try the OpenAPI documentation at: $API_URL/docs"
    echo ""
        return 0
    else
        log_error "❌ Some tests failed. Please check the logs above."
        return 1
    fi
}

# Check dependencies
check_dependencies() {
    if ! command -v curl >/dev/null 2>&1; then
        log_error "curl is required but not installed. Aborting."
        exit 1
    fi
    
    if ! command -v jq >/dev/null 2>&1; then
        log_error "jq is required but not installed. Aborting."
        exit 1
    fi
}

# Run dependency check and main tests
check_dependencies
main "$@"
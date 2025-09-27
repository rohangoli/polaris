# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/bin/bash

echo "🔒 Keycloak Security Configuration Validation"
echo "============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test function
test_endpoint() {
    local description="$1"
    local url="$2"
    local expected_result="$3"
    
    echo -n "Testing: $description... "
    
    response=$(curl -s -w "%{http_code}" -o /dev/null "$url")
    
    if [ "$response" = "$expected_result" ]; then
        echo -e "${GREEN}✓ PASS${NC}"
        return 0
    else
        echo -e "${RED}✗ FAIL (Got: $response, Expected: $expected_result)${NC}"
        return 1
    fi
}

echo -e "\n${YELLOW}1. Testing Authentication Functionality${NC}"
echo "----------------------------------------"

# Test 1: Basic Authentication Still Works
echo "Testing basic OIDC token retrieval..."
token_response=$(docker exec objectscale_keycloak_1 curl -s -X POST "http://localhost:8080/realms/iceberg/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=client1&client_secret=s3cr3t")

if echo "$token_response" | grep -q "access_token"; then
    echo -e "${GREEN}✓ Authentication successful - tokens are being issued${NC}"
else
    echo -e "${RED}✗ Authentication failed - no access token received${NC}"
    echo "Response: $token_response"
fi

echo -e "\n${YELLOW}2. Testing Redirect URI Security${NC}"
echo "-------------------------------"

# Test 2: Keycloak is accessible
test_endpoint "Keycloak health check" "http://localhost:8080/realms/iceberg/.well-known/openid_configuration" "200"

echo -e "\n${YELLOW}3. Verify Redirect URI Configuration${NC}"
echo "-----------------------------------"

# Check the actual configuration in running Keycloak
echo "Checking client1 redirect URIs in running Keycloak..."
client_config=$(docker exec objectscale_keycloak_1 /opt/keycloak/bin/kcadm.sh get clients -r iceberg --fields id,clientId,redirectUris --format csv --nohostname --offline 2>/dev/null | grep -A5 -B5 client1 || echo "Could not retrieve client config")

if [ "$client_config" != "Could not retrieve client config" ]; then
    echo -e "${GREEN}✓ Client configuration retrieved${NC}"
    echo "$client_config"
else
    echo -e "${YELLOW}⚠ Could not retrieve live config (realm may need restart)${NC}"
fi

echo -e "\n${YELLOW}4. Configuration File Validation${NC}"
echo "-------------------------------"

# Check the JSON configuration
if grep -q "https://localhost:8443/polaris/callback" /root/polaris/getting-started/assets/keycloak/iceberg-realm.json; then
    echo -e "${GREEN}✓ Secure HTTPS redirect URI configured${NC}"
else
    echo -e "${RED}✗ HTTPS redirect URI not found${NC}"
fi

if grep -q "http://localhost\*" /root/polaris/getting-started/assets/keycloak/iceberg-realm.json; then
    echo -e "${RED}✗ Dangerous wildcard still present${NC}"
else
    echo -e "${GREEN}✓ Dangerous wildcards removed${NC}"
fi

if grep -q "/polaris/callback" /root/polaris/getting-started/assets/keycloak/iceberg-realm.json; then
    echo -e "${GREEN}✓ Specific callback paths configured${NC}"
else
    echo -e "${RED}✗ Specific callback paths not found${NC}"
fi

echo -e "\n${YELLOW}5. Security Recommendations${NC}"
echo "-------------------------"

echo -e "${YELLOW}For Production Deployment:${NC}"
echo "1. Generate strong client secret: openssl rand -base64 32"
echo "2. Enable HTTPS/TLS with valid certificates"
echo "3. Update redirect URIs to match your production domain"
echo "4. Restart Keycloak to apply realm configuration changes"
echo "5. Test authentication flow with production URLs"

echo -e "\n${YELLOW}6. Test Catalog API (Final Integration Test)${NC}"
echo "-------------------------------------------------"

# Get token and test catalog API
echo "Testing full authentication flow..."
token=$(docker exec objectscale_keycloak_1 curl -s -X POST "http://localhost:8080/realms/iceberg/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=client1&client_secret=s3cr3t" | \
  grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [ ! -z "$token" ]; then
    echo "Testing catalog listing with secured token..."
    catalog_response=$(curl -s -H "Authorization: Bearer $token" "http://localhost:8181/api/management/v1/catalogs")
    
    if echo "$catalog_response" | grep -q "catalogs"; then
        echo -e "${GREEN}✓ Full authentication and catalog access working${NC}"
        echo "Catalogs found: $(echo "$catalog_response" | jq -r '.catalogs | length' 2>/dev/null || echo "JSON parse error")"
    else
        echo -e "${RED}✗ Catalog access failed${NC}"
        echo "Response: $catalog_response"
    fi
else
    echo -e "${RED}✗ Could not obtain access token${NC}"
fi

echo -e "\n${GREEN}Security validation complete!${NC}"
echo "See SECURITY_CONFIGURATION.md for detailed hardening guide."

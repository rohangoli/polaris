<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Polaris with Dell ObjectScale and Keycloak

This setup demonstrates how to run Apache Polaris with Dell ObjectScale storage and Keycloak authentication.

## Overview

This configuration combines:
- **Apache Polaris**: Apache Iceberg catalog service
- **Dell ObjectScale**: Enterprise object storage providing S3-compatible API
- **Keycloak**: Identity and access management for OIDC authentication

## Prerequisites

- Docker and Docker Compose installed
- Access to Dell ObjectScale endpoints
- Valid ObjectScale credentials (access key ID and secret access key)

## Configuration

### ObjectScale Settings

The setup is configured to use Dell ObjectScale with the following endpoints:
- **S3 Endpoint**: `ecs-lb.example.com`
- **STS Endpoint**: `https://ecs.example.com:4443/sts`
- **Role ARN**: `urn:ecs:iam::test-namespace:role/test-role`
- **Path Style Access**: `true` (Dell ObjectScale uses S3 path-style access)
- **SSL Verification**: `ignoreSSLVerification: true` - **WARNING: This disables SSL certificate verification and should only be used in development environments with self-signed certificates. Do not use in production.**

### Authentication Realms

This setup supports multiple authentication realms:
- `realm-internal`: Internal Polaris authentication
- `realm-external`: External OIDC authentication via Keycloak
- `realm-mixed`: Mixed authentication (both internal and external)

## Services

### PostgreSQL
- **Port**: 5432
- **Database**: `POLARIS`
- **Username**: `postgres`
- **Password**: `postgres`

### Polaris Bootstrap
- **Image**: `apache/polaris-admin-tool:latest`
- **Purpose**: Initializes Polaris with configured realms and credentials

### Polaris
- **Image**: `apache/polaris:latest`
- **API Port**: 8181
- **Management Port**: 8182 (metrics and health checks)
- **Debug Port**: 5005 (optional, for JVM debugging)

### Polaris Setup
- **Image**: `alpine/curl`
- **Purpose**: Creates initial catalog configuration with ObjectScale storage

### Keycloak
- **Image**: `quay.io/keycloak/keycloak:26.3.5`
- **Port**: 8080
- **Admin Username**: `admin`
- **Admin Password**: `admin`
- **Realm**: `iceberg`

## Starting the Example

1. Build the Polaris server image if it's not already present locally:

    ```shell
      ./gradlew \
      :polaris-server:assemble \
      :polaris-server:quarkusAppPartsBuild --rerun \
      :polaris-admin:assemble \
      :polaris-admin:quarkusAppPartsBuild --rerun \
      -Dquarkus.container-image.build=true
    ```

2. Start the docker compose group by running the following command from the root of the repository:

    ```shell
    docker compose -f getting-started/objectscale/docker-compose.yml up -d
    ```

## Usage

### Starting the Services

1. Navigate to the ObjectScale directory:
   ```bash
   cd getting-started/objectscale
   ```

2. Start all services:
   ```bash
   docker-compose up -d
   ```

3. Wait for services to be healthy:
   ```bash
   docker-compose logs -f polaris-setup
   ```

4. **Optional: Run API Tests**
   
   The setup includes comprehensive REST API tests that validate all Polaris operations:

   ```bash
   # Run all tests (default)
   docker compose -f getting-started/objectscale/docker-compose.yml up polaris-api-tests

   # Run quick validation tests
   TEST_MODE=quick docker compose -f getting-started/objectscale/docker-compose.yml up polaris-api-tests

   # Run catalog-only tests
   TEST_MODE=catalogs docker compose -f getting-started/objectscale/docker-compose.yml up polaris-api-tests

   # Check test results
   docker compose -f getting-started/objectscale/docker-compose.yml logs polaris-api-tests
   ```

   The API tests cover:
   - **Catalogs**: Create, Update, List, Delete
   - **Namespaces**: Create, List, Load metadata, Check exists, Create nested, Drop, Update  
   - **Tables**: Create, List, Load, Exists check, Drop, Register, Rename
   - **Views**: Create, List, Exists check, Load, Replace, Drop, Rename

### Accessing Services

- **PostgreSQL**: localhost:5432
- **Polaris API**: http://localhost:8181
- **Polaris Management**: http://localhost:8182
- **Keycloak Console**: http://localhost:8080

### Authentication

#### Internal Authentication
For internal realms, use the bootstrap credentials:
- **Username**: `root`
- **Password**: `s3cr3t`

#### External Authentication (OIDC)
For external authentication, obtain a token from Keycloak:
```bash
# From within Docker environment
docker exec -it objectscale-keycloak-1 \
  curl -X POST http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
  --user client1:s3cr3t \
  -d 'grant_type=client_credentials' | jq -r .access_token

# From host machine
curl -X POST http://localhost:8080/realms/iceberg/protocol/openid-connect/token \
  --user client1:s3cr3t \
  -d 'grant_type=client_credentials' | jq -r .access_token
```

### Testing the Setup

1. **Health Check**:
   ```bash
   curl http://localhost:8182/q/health
   ```

2. **List Catalogs**:
   
   This setup supports three different authentication realms. Choose the appropriate method based on your requirements:

   #### Internal Authentication (realm-internal)
   Uses Polaris internal authentication with basic auth credentials:
   ```bash
   curl -u root:s3cr3t \
     -H "Polaris-Realm: realm-internal" \
     http://localhost:8181/api/management/v1/catalogs
   ```

   #### External Authentication (realm-external)
   Uses Keycloak OIDC tokens exclusively. Get token from host machine:
   ```bash
   # Get OIDC token from Keycloak (from within Docker environment)
   TOKEN=$(docker exec -it objectscale-keycloak-1 \
     curl -X POST http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
     --user client1:s3cr3t \
     -d 'grant_type=client_credentials' | jq -r .access_token)
   
   # Alternative: Get token from host machine
   TOKEN=$(curl -X POST http://localhost:8080/realms/iceberg/protocol/openid-connect/token \
     --user client1:s3cr3t \
     -d 'grant_type=client_credentials' | jq -r .access_token)
   
   # Use token with external realm
   curl -H "Authorization: Bearer $TOKEN" \
     -H "Polaris-Realm: realm-external" \
     http://localhost:8181/api/management/v1/catalogs
   ```

   #### Mixed Authentication (realm-mixed)
   Supports both internal credentials and OIDC tokens:
   ```bash
   # Option 1: Using internal credentials
   curl -u root:s3cr3t \
     -H "Polaris-Realm: realm-mixed" \
     http://localhost:8181/api/management/v1/catalogs
   
   # Option 2: Using OIDC token (from within Docker environment)
   TOKEN=$(docker exec -it objectscale-keycloak-1 \
     curl -X POST http://keycloak:8080/realms/iceberg/protocol/openid-connect/token \
     --user client1:s3cr3t \
     -d 'grant_type=client_credentials' | jq -r .access_token)
   
   # Alternative: Get token from host machine
   TOKEN=$(curl -X POST http://localhost:8080/realms/iceberg/protocol/openid-connect/token \
     --user client1:s3cr3t \
     -d 'grant_type=client_credentials' | jq -r .access_token)
   
   curl -H "Authorization: Bearer $TOKEN" \
     -H "Polaris-Realm: realm-mixed" \
     http://localhost:8181/api/management/v1/catalogs
   ```

   **Notes**: 
   - The `Polaris-Realm` header is required to specify which authentication realm to use. If omitted, requests may fail or use unexpected authentication methods.
   - When running commands from within the Docker environment, use `keycloak:8080` for internal container communication.
   - When running commands from the host machine, use `localhost:8080` to access Keycloak through the exposed port.
   - The Docker container name format follows the pattern `{directory}-{service}-{instance}`, so for this setup it's `objectscale-keycloak-1`.

## Storage Configuration

The ObjectScale storage is configured with:
- **Storage Type**: S3
- **Endpoint**: Dell ObjectScale S3 endpoint (`ecs-lb.example.com`)
- **STS Endpoint**: Dell ObjectScale STS endpoint for temporary credentials (`https://ecs.example.com:4443/sts`)
- **Role ARN**: `urn:ecs:iam::otf_dev:role/assumeRoleSameAccountOTF`
- **Path Style Access**: Enabled (required for Dell ObjectScale)
- **Default Storage Location**: `s3://polaris`

## Environment Variables

Key environment variables used:

### AWS/ObjectScale Configuration
- `AWS_ACCESS_KEY_ID`: ObjectScale access key
- `AWS_SECRET_ACCESS_KEY`: ObjectScale secret key
- `AWS_REGION`: AWS region (us-east-1)

### PostgreSQL Configuration
- `POSTGRES_USER`: postgres
- `POSTGRES_PASSWORD`: postgres
- `POSTGRES_DB`: POLARIS
- `QUARKUS_DATASOURCE_JDBC_URL`: jdbc:postgresql://postgres:5432/POLARIS

### Keycloak Configuration
- `KC_BOOTSTRAP_ADMIN_USERNAME`: admin
- `KC_BOOTSTRAP_ADMIN_PASSWORD`: admin

### Polaris Configuration
- `POLARIS_PERSISTENCE_TYPE`: relational-jdbc
- `POLARIS_BOOTSTRAP_CREDENTIALS`: Credentials for all realms

## Security Hardening for Production

### Keycloak Security Configuration

**⚠️ WARNING**: This development setup uses default credentials and insecure settings. For production deployments, implement the following security measures:

#### 1. Admin Credentials
```bash
# Change default admin credentials
KC_BOOTSTRAP_ADMIN_USERNAME=<strong-admin-username>
KC_BOOTSTRAP_ADMIN_PASSWORD=<strong-admin-password>
```

#### 2. Client Secrets
- Replace the default client secret (`s3cr3t`) with a strong, randomly generated secret
- Use at least 32 characters with mixed case, numbers, and special characters
- Update the client configuration in Keycloak admin console under:
  `Clients → client1 → Credentials → Client Secret`

#### 3. SSL/TLS Configuration
```yaml
# Enable HTTPS in production
environment:
  KC_HTTPS_CERTIFICATE_FILE: /opt/keycloak/conf/server.crt.pem
  KC_HTTPS_CERTIFICATE_KEY_FILE: /opt/keycloak/conf/server.key.pem
  KC_HOSTNAME: your-keycloak-domain.com
  KC_HOSTNAME_STRICT: true
volumes:
  - ./certs/server.crt.pem:/opt/keycloak/conf/server.crt.pem:ro
  - ./certs/server.key.pem:/opt/keycloak/conf/server.key.pem:ro
```

#### 4. Database Security
- Use dedicated database user with minimal privileges
- Enable SSL connection between Keycloak and database
- Regular database backups with encryption

#### 5. Token Security
```bash
# Reduce token lifespans for production
# In Keycloak Admin Console → Realm Settings → Tokens:
# - Access Token Lifespan: 5 minutes (300 seconds)
# - Refresh Token Lifespan: 30 minutes
# - Client Session Idle: 30 minutes
# - Client Session Max: 10 hours
```

#### 6. Network Security
- Use reverse proxy (nginx/Apache) with SSL termination
- Implement IP whitelisting for admin access
- Configure proper CORS policies
- Use private networks for inter-service communication

#### 7. Realm Configuration Security
```json
{
  "bruteForceProtected": true,
  "maxFailureWaitSeconds": 900,
  "minimumQuickLoginWaitSeconds": 60,
  "waitIncrementSeconds": 60,
  "quickLoginCheckMilliSeconds": 1000,
  "maxDeltaTimeSeconds": 43200,
  "failureFactor": 30,
  "resetPasswordAllowed": false,
  "rememberMe": false,
  "verifyEmail": true,
  "loginWithEmailAllowed": false,
  "duplicateEmailsAllowed": false
}
```

#### 8. Client Security Settings
- Set `standardFlowEnabled: false` if not using authorization code flow
- Configure `validRedirectUris` with exact URLs (avoid wildcards)
- Set `webOrigins` to specific domains only
- Enable `fullScopeAllowed: false` and assign minimal required scopes
- Use `confidential` access type for server-side applications

#### 9. Monitoring and Auditing
```bash
# Enable event logging
# In Keycloak Admin Console → Events → Config:
# - Save Events: ON
# - Include Representation: OFF (for sensitive data)
# - Expiration: 30 days
# - Events: LOGIN, LOGIN_ERROR, LOGOUT, UPDATE_PASSWORD, etc.
```

#### 10. RSA Key Pair Security
```bash
# Use stronger RSA keys (4096-bit minimum)
openssl genpkey -algorithm RSA -out private.key -pkeyopt rsa_keygen_bits:4096
openssl rsa -in private.key -pubout -out public.key

# Set proper file permissions
chmod 600 private.key
chmod 644 public.key
chown polaris:polaris private.key public.key
```

### ObjectScale Security

#### 1. Credentials Management
- Use IAM roles instead of static access keys when possible
- Rotate access keys regularly (every 90 days recommended)
- Store credentials in secure secret management systems (HashiCorp Vault, AWS Secrets Manager)

#### 2. SSL/TLS Configuration
```bash
# Enable SSL verification in production
polaris.features."ALLOW_INSECURE_STORAGE_TYPES": "false"
# Remove ignoreSSLVerification from storage configuration
```

#### 3. Network Security
- Use private network connectivity between Polaris and ObjectScale
- Implement VPC/network segmentation
- Configure firewall rules to restrict access to necessary ports only

### General Security Best Practices

1. **Container Security**:
   - Use non-root users in containers
   - Scan images for vulnerabilities regularly
   - Use minimal base images (distroless, alpine)
   - Set resource limits and security contexts

2. **Secret Management**:
   - Use Docker secrets or Kubernetes secrets instead of environment variables
   - Implement secret rotation policies
   - Never commit secrets to version control

3. **Monitoring**:
   - Enable comprehensive logging for all services
   - Set up alerting for security events
   - Monitor for unauthorized access attempts
   - Implement log aggregation and analysis

4. **Updates**:
   - Keep all components updated with latest security patches
   - Subscribe to security advisories for used components
   - Implement automated vulnerability scanning

## Troubleshooting

### Common Issues

1. **Services not starting**: Check if ports 5432, 8080, 8181, 8182 are available
2. **PostgreSQL connectivity**: Verify database is running and accessible
3. **ObjectScale connectivity**: Verify endpoint URLs and credentials
4. **Authentication failures**: Check Keycloak realm configuration
5. **Bootstrap failures**: Check polaris-bootstrap service logs for realm initialization issues

### Logs

View service logs:
```bash
# All services
docker-compose logs -f

# Specific services
docker-compose logs -f postgres
docker-compose logs -f polaris-bootstrap
docker-compose logs -f polaris
docker-compose logs -f polaris-setup
docker-compose logs -f keycloak
```

### Cleanup

Stop and remove all services:
```bash
docker-compose down -v
docker-compose down -v --remove-orphans
```
or
```bash
docker compose -f getting-started/objectscale/docker-compose.yml down -v
```

## Notes

- Dell ObjectScale requires S3 path-style access, which is properly configured in this setup
- The setup includes both STS and S3 endpoints for full ObjectScale integration with role-based access
- Multiple authentication realms (internal, external, mixed) allow flexibility in access patterns
- PostgreSQL is used for Polaris metadata persistence instead of in-memory storage
- Health checks ensure services are ready before dependent services start
- The polaris-setup service automatically creates the initial catalog with ObjectScale storage configuration
- SSL verification is disabled for development use with self-signed certificates
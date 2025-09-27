#!/bin/bash

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
#

set -e

# Default key directory
KEY_DIR=${1:-"./keys"}
PRIVATE_KEY_FILE="${KEY_DIR}/private.key"
PUBLIC_KEY_FILE="${KEY_DIR}/public.key"

echo "Generating RSA key-pair for Polaris token broker authentication..."

# Create key directory if it doesn't exist
mkdir -p "$KEY_DIR"

# Check if keys already exist
if [ -f "$PRIVATE_KEY_FILE" ] && [ -f "$PUBLIC_KEY_FILE" ]; then
    echo "RSA key-pair already exists in $KEY_DIR"
    echo "Private key: $PRIVATE_KEY_FILE"
    echo "Public key:  $PUBLIC_KEY_FILE"
    exit 0
fi

# Generate RSA private key in PKCS#8 format (2048 bits)
echo "Generating RSA private key..."
openssl genpkey -algorithm RSA -out "$PRIVATE_KEY_FILE" -pkeyopt rsa_keygen_bits:2048

# Extract public key from private key
echo "Extracting public key..."
openssl rsa -in "$PRIVATE_KEY_FILE" -pubout -out "$PUBLIC_KEY_FILE"

# Set appropriate permissions
chmod 600 "$PRIVATE_KEY_FILE"
chmod 644 "$PUBLIC_KEY_FILE"

echo "RSA key-pair generated successfully!"
echo "Private key: $PRIVATE_KEY_FILE (permissions: 600)"
echo "Public key:  $PUBLIC_KEY_FILE (permissions: 644)"
echo ""
echo "Use these keys in your Polaris configuration:"
echo "polaris.authentication.token-broker.type=rsa-key-pair"
echo "polaris.authentication.token-broker.rsa-key-pair.private-key-file=$PRIVATE_KEY_FILE"
echo "polaris.authentication.token-broker.rsa-key-pair.public-key-file=$PUBLIC_KEY_FILE"

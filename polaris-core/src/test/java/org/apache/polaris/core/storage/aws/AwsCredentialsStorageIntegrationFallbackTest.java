/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

public class AwsCredentialsStorageIntegrationFallbackTest {

  @Test
  public void testFallbackUsesEcsXmlParserWhenSdkReturnsNull() throws Exception {
    // Prepare a minimal AwsStorageConfigurationInfo
    AwsStorageConfigurationInfo cfg =
        AwsStorageConfigurationInfo.builder()
            .roleARN("urn:ecs:iam::test-namespace:role/test-role")
            .endpoint("https://ecs-lb.example.com")
            .pathStyleAccess(true)
            .stsEndpoint("https://ecs.example.com:4443/sts")
            .region("us-east-1")
            .build();

    // Create a StsClientProvider that returns a mock StsClient whose assumeRole returns
    // an AssumeRoleResponse with null credentials
    StsClient mockClient = mock(StsClient.class);
    org.mockito.Mockito.doReturn(AssumeRoleResponse.builder().build())
        .when(mockClient)
        .assumeRole(
            org.mockito.ArgumentMatchers.any(
                software.amazon.awssdk.services.sts.model.AssumeRoleRequest.class));

    // ecsXml used by the provider below
    String ecsXml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            + "<ns2:AssumeRoleResponse xmlns:ns2=\"none\">"
            + "<AssumeRoleResult><Credentials><AccessKeyId>AKID123</AccessKeyId>"
            + "<SecretAccessKey>SK123</SecretAccessKey><SessionToken>TOKEN123</SessionToken>"
            + "<Expiration>2025-09-28T16:24:00Z</Expiration></Credentials></AssumeRoleResult>"
            + "</ns2:AssumeRoleResponse>";

    // Provide a StsClientProvider that returns our mock client and exposes the raw STS XML
    StsClientProvider provider =
        new StsClientProvider() {
          @Override
          public StsClient stsClient(StsClientProvider.StsDestination destination) {
            return mockClient;
          }

          @Override
          public java.util.Optional<String> lastRawBody() {
            return java.util.Optional.of(ecsXml);
          }
        };

    AwsCredentialsStorageIntegration integ =
        new AwsCredentialsStorageIntegration(cfg, provider, Optional.empty());

    // legacy system property is not required; provider exposes raw body

    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(
            org.apache.polaris.core.config.FeatureConfiguration
                .STORAGE_CREDENTIAL_DURATION_SECONDS))
        .thenReturn(900);

    var accessConfig =
        integ.getSubscopedCreds(
            realmConfig, false, java.util.Set.of(), java.util.Set.of(), Optional.empty());

    assertThat(accessConfig.get(StorageAccessProperty.AWS_KEY_ID)).isEqualTo("AKID123");
    assertThat(accessConfig.get(StorageAccessProperty.AWS_SECRET_KEY)).isEqualTo("SK123");
    assertThat(accessConfig.get(StorageAccessProperty.AWS_TOKEN)).isEqualTo("TOKEN123");
    // Ensure fallback also preserves endpoint/path-style so downstream FileIO can honor
    // the S3-compatible endpoint and path-style addressing.
    assertThat(accessConfig.get(StorageAccessProperty.AWS_ENDPOINT))
        .isEqualTo("https://ecs-lb.example.com");
    assertThat(accessConfig.get(StorageAccessProperty.AWS_PATH_STYLE_ACCESS))
        .isEqualTo(Boolean.TRUE.toString());
  }
}

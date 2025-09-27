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

import java.net.URI;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class AwsStorageConfigurationInfoTest {

  @Test
  public void testStsEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(null, URI.create("http://sts.example.com"));
    assertThat(newBuilder().endpoint("http://s3.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://s3.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .stsEndpoint("http://sts.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://sts.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getStsEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(
            URI.create("http://s3.example.com"),
            URI.create("http://int.example.com"),
            URI.create("http://int.example.com"));
  }

  private static ImmutableAwsStorageConfigurationInfo.Builder newBuilder() {
    return AwsStorageConfigurationInfo.builder()
        .roleARN("arn:aws:iam::123456789012:role/polaris-test");
  }

  @Test
  public void testInternalEndpoint() {
    assertThat(newBuilder().build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().stsEndpoint("http://sts.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, null);
    assertThat(newBuilder().endpoint("http://s3.example.com").build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://s3.example.com"));
    assertThat(
            newBuilder()
                .endpoint("http://s3.example.com")
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(URI.create("http://s3.example.com"), URI.create("http://int.example.com"));
    assertThat(
            newBuilder()
                .stsEndpoint("http://sts.example.com")
                .endpointInternal("http://int.example.com")
                .build())
        .extracting(
            AwsStorageConfigurationInfo::getEndpointUri,
            AwsStorageConfigurationInfo::getInternalEndpointUri)
        .containsExactly(null, URI.create("http://int.example.com"));
  }

  @Test
  public void testPathStyleAccess() {
    assertThat(newBuilder().pathStyleAccess(null).build().getPathStyleAccess()).isNull();
    assertThat(newBuilder().pathStyleAccess(false).build().getPathStyleAccess()).isFalse();
    assertThat(newBuilder().pathStyleAccess(true).build().getPathStyleAccess()).isTrue();
  }

  @Test
  public void testRoleArnParsing() {
    AwsStorageConfigurationInfo awsConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://bucket/path/to/warehouse")
            .roleARN("arn:aws:iam::012345678901:role/jdoe")
            .region("us-east-2")
            .build();

    Assertions.assertThat(awsConfig)
        .extracting(
            AwsStorageConfigurationInfo::getRoleARN,
            AwsStorageConfigurationInfo::getAwsAccountId,
            AwsStorageConfigurationInfo::getAwsPartition)
        .containsExactly("arn:aws:iam::012345678901:role/jdoe", "012345678901", "aws");
  }

  @Test
  public void testS3CompatibleArnParsing() {
    AwsStorageConfigurationInfo s3Config =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://bucket/path/to/warehouse")
            .roleARN("urn:ecs:iam::namespace:role/test-role")
            .region("us-east-2")
            .build();

    Assertions.assertThat(s3Config)
        .extracting(
            AwsStorageConfigurationInfo::getRoleARN,
            AwsStorageConfigurationInfo::getAwsAccountId,
            AwsStorageConfigurationInfo::getAwsPartition)
        .containsExactly("urn:ecs:iam::namespace:role/test-role", "namespace", "ecs");
  }

  @Test
  public void testAwsUsGovArnParsing() {
    AwsStorageConfigurationInfo awsGovConfig =
        AwsStorageConfigurationInfo.builder()
            .addAllowedLocation("s3://bucket/path/to/warehouse")
            .roleARN("arn:aws-us-gov:iam::012345678901:role/gov-role")
            .region("us-gov-east-1")
            .build();

    Assertions.assertThat(awsGovConfig)
        .extracting(
            AwsStorageConfigurationInfo::getRoleARN,
            AwsStorageConfigurationInfo::getAwsAccountId,
            AwsStorageConfigurationInfo::getAwsPartition)
        .containsExactly(
            "arn:aws-us-gov:iam::012345678901:role/gov-role", "012345678901", "aws-us-gov");
  }

  @Test
  public void testArnValidation() {
    // Test valid AWS ARN
    Assertions.assertThatCode(
            () ->
                AwsStorageConfigurationInfo.validateArn("arn:aws:iam::123456789012:role/test-role"))
        .doesNotThrowAnyException();

    // Test valid AWS US Gov ARN
    Assertions.assertThatCode(
            () ->
                AwsStorageConfigurationInfo.validateArn(
                    "arn:aws-us-gov:iam::123456789012:role/test-role"))
        .doesNotThrowAnyException();

    // Test valid S3-compatible ARN
    Assertions.assertThatCode(
            () -> AwsStorageConfigurationInfo.validateArn("urn:ecs:iam::namespace:role/test-role"))
        .doesNotThrowAnyException();

    // Test null ARN (should be allowed)
    Assertions.assertThatCode(() -> AwsStorageConfigurationInfo.validateArn(null))
        .doesNotThrowAnyException();

    // Test empty ARN (should fail)
    Assertions.assertThatThrownBy(() -> AwsStorageConfigurationInfo.validateArn(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("ARN must not be empty");

    // Test invalid ARN format (should fail)
    Assertions.assertThatThrownBy(
            () -> AwsStorageConfigurationInfo.validateArn("invalid:arn:format"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid role ARN format");

    // Test AWS China ARN (should fail)
    Assertions.assertThatThrownBy(
            () ->
                AwsStorageConfigurationInfo.validateArn(
                    "arn:aws-cn:iam::123456789012:role/test-role"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("AWS China is temporarily not supported");
  }
}

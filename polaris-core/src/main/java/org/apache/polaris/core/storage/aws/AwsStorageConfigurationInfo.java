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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Aws Polaris Storage Configuration information */
@PolarisImmutable
@JsonSerialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonDeserialize(as = ImmutableAwsStorageConfigurationInfo.class)
@JsonTypeName("AwsStorageConfigurationInfo")
public abstract class AwsStorageConfigurationInfo extends PolarisStorageConfigurationInfo {

  public static ImmutableAwsStorageConfigurationInfo.Builder builder() {
    return ImmutableAwsStorageConfigurationInfo.builder();
  }

  // Support both AWS ARN (arn:aws:iam::123456789012:role/test-role) and S3-Compatible ARN
  // (urn:ecs:iam::namespace:role/test-role)
  @JsonIgnore
  public static final String ROLE_ARN_PATTERN =
      "^(arn:(aws|aws-us-gov):iam::(\\d{12})|urn:ecs:iam::([a-zA-Z0-9_]+)):role/.+$";

  private static final Pattern ROLE_ARN_PATTERN_COMPILED = Pattern.compile(ROLE_ARN_PATTERN);

  @Override
  public StorageType getStorageType() {
    return StorageType.S3;
  }

  @Override
  public String getFileIoImplClassName() {
    return "org.apache.iceberg.aws.s3.S3FileIO";
  }

  @Nullable
  public abstract String getRoleARN();

  /** AWS external ID, optional */
  @Nullable
  public abstract String getExternalId();

  /** User ARN for the service principal */
  @Nullable
  public abstract String getUserARN();

  /** AWS region */
  @Nullable
  public abstract String getRegion();

  /** Endpoint URI for S3 API calls */
  @Nullable
  public abstract String getEndpoint();

  /** Internal endpoint URI for S3 API calls */
  @Nullable
  public abstract String getEndpointInternal();

  @JsonIgnore
  @Nullable
  public URI getEndpointUri() {
    return normalizeToUri(getEndpoint());
  }

  @JsonIgnore
  @Nullable
  public URI getInternalEndpointUri() {
    return normalizeToUri(getEndpointInternal()) == null
        ? getEndpointUri()
        : normalizeToUri(getEndpointInternal());
  }

  /**
   * Normalize a possibly-relative or scheme-less endpoint string into an absolute URI. If the input
   * is null or cannot be parsed, returns null. If the input has no scheme, default to https scheme.
   */
  @JsonIgnore
  @Nullable
  private static URI normalizeToUri(@Nullable String maybe) {
    if (maybe == null) {
      return null;
    }
    try {
      URI uri = URI.create(maybe);
      if (uri.getScheme() == null || uri.getScheme().isEmpty()) {
        // default to https when no scheme provided
        return URI.create("https://" + maybe);
      }
      return uri;
    } catch (Exception e) {
      try {
        return URI.create("https://" + maybe);
      } catch (Exception ex) {
        return null;
      }
    }
  }

  /** Flag indicating whether path-style bucket access should be forced in S3 clients. */
  public abstract @Nullable Boolean getPathStyleAccess();

  /** Endpoint URI for STS API calls */
  @Nullable
  public abstract String getStsEndpoint();

  /** Flag indicating whether SSL certificate verification should be disabled */
  public abstract @Nullable Boolean getIgnoreSSLVerification();

  /** Returns the STS endpoint if set, defaulting to {@link #getEndpointUri()} otherwise. */
  @JsonIgnore
  @Nullable
  public URI getStsEndpointUri() {
    return getStsEndpoint() == null ? getInternalEndpointUri() : URI.create(getStsEndpoint());
  }

  @JsonIgnore
  @Nullable
  public String getAwsAccountId() {
    String arn = getRoleARN();
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      checkState(matcher.matches());
      // Group 3 for AWS ARN (arn:aws:iam::123456789012:role/...), Group 4 for S3-compatible ARN
      // (urn:ecs:iam::otf_dev:role/...)
      return matcher.group(3) != null ? matcher.group(3) : matcher.group(4);
    }
    return null;
  }

  @JsonIgnore
  @Nullable
  public String getAwsPartition() {
    String arn = getRoleARN();
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      checkState(matcher.matches());
      // Group 2 for AWS ARN partition (aws|aws-us-gov), return "ecs" for S3-compatible ARN
      return matcher.group(2) != null ? matcher.group(2) : "ecs";
    }
    return null;
  }

  @Value.Check
  @Override
  protected void check() {
    super.check();
    String arn = getRoleARN();
    validateArn(arn);
    if (arn != null) {
      Matcher matcher = ROLE_ARN_PATTERN_COMPILED.matcher(arn);
      if (!matcher.matches()) {
        throw new IllegalArgumentException("ARN does not match the expected role ARN pattern");
      }
    }
  }

  public static void validateArn(String arn) {
    if (arn == null) {
      return;
    }
    if (arn.isEmpty()) {
      throw new IllegalArgumentException("ARN must not be empty");
    }
    // specifically throw errors for China
    if (arn.contains("aws-cn")) {
      throw new IllegalArgumentException("AWS China is temporarily not supported");
    }
    checkArgument(Pattern.matches(ROLE_ARN_PATTERN, arn), "Invalid role ARN format: %s", arn);
  }
}

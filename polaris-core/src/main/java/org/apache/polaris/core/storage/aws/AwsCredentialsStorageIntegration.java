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

import static org.apache.polaris.core.config.FeatureConfiguration.STORAGE_CREDENTIAL_DURATION_SECONDS;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.secrets.EcsXmlParser;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.core.storage.aws.StsClientProvider.StsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.policybuilder.iam.IamConditionOperator;
import software.amazon.awssdk.policybuilder.iam.IamEffect;
import software.amazon.awssdk.policybuilder.iam.IamPolicy;
import software.amazon.awssdk.policybuilder.iam.IamResource;
import software.amazon.awssdk.policybuilder.iam.IamStatement;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

/** Credential vendor that supports generating */
public class AwsCredentialsStorageIntegration
    extends InMemoryStorageIntegration<AwsStorageConfigurationInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(AwsCredentialsStorageIntegration.class);
  private final StsClientProvider stsClientProvider;
  private final Optional<AwsCredentialsProvider> credentialsProvider;

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config, StsClient fixedClient) {
    this(config, (destination) -> fixedClient);
  }

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config, StsClientProvider stsClientProvider) {
    this(config, stsClientProvider, Optional.empty());
  }

  public AwsCredentialsStorageIntegration(
      AwsStorageConfigurationInfo config,
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> credentialsProvider) {
    super(config, AwsCredentialsStorageIntegration.class.getName());
    this.stsClientProvider = stsClientProvider;
    this.credentialsProvider = credentialsProvider;
  }

  /** {@inheritDoc} */
  @Override
  public AccessConfig getSubscopedCreds(
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    int storageCredentialDurationSeconds =
        realmConfig.getConfig(STORAGE_CREDENTIAL_DURATION_SECONDS);
    AwsStorageConfigurationInfo storageConfig = config();
    String region = storageConfig.getRegion();
    AccessConfig.Builder accessConfig = AccessConfig.builder();

    if (shouldUseSts(storageConfig)) {
      AssumeRoleRequest.Builder request =
          AssumeRoleRequest.builder()
              .externalId(storageConfig.getExternalId())
              .roleArn(storageConfig.getRoleARN())
              .roleSessionName("PolarisAwsCredentialsStorageIntegration")
              .policy(
                  policyString(
                          storageConfig.getAwsPartition(),
                          allowListOperation,
                          allowedReadLocations,
                          allowedWriteLocations)
                      .toJson())
              .durationSeconds(storageCredentialDurationSeconds);
      credentialsProvider.ifPresent(
          cp -> request.overrideConfiguration(b -> b.credentialsProvider(cp)));

      @SuppressWarnings("resource")
      // Note: stsClientProvider returns "thin" clients that do not need closing
      StsClient stsClient =
          stsClientProvider.stsClient(
              StsDestination.of(
                  storageConfig.getStsEndpointUri(),
                  region,
                  storageConfig.getIgnoreSSLVerification()));

    AssumeRoleResponse response;
    try {
      response = stsClient.assumeRole(request.build());
    } catch (software.amazon.awssdk.services.sts.model.StsException se) {
      String msg = se.getMessage() == null ? "" : se.getMessage();
      LOG.warn("AssumeRole failed: {}. Attempting fallback policy with AWS arn prefix.", msg);
      if (msg.contains("Policy has invalid resource") || msg.contains("invalid resource")) {
        // retry with AWS-style ARNs only (force partition to 'aws')
        final String fallbackPolicyJson =
            policyString("aws", allowListOperation, allowedReadLocations, allowedWriteLocations)
                .toJson();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Retrying AssumeRole with fallback policy: {}", fallbackPolicyJson);
        }
        AssumeRoleRequest retryRequest = request.policy(fallbackPolicyJson).build();
        response = stsClient.assumeRole(retryRequest);
      } else {
        throw se;
      }
    }
    // Some STS endpoints (especially custom/on-prem ECS variants) may return a successful
    // HTTP response but the SDK-parsed AssumeRoleResponse.credentials() may be null.
    // Detect that and attempt a single retry using the AWS-style fallback policy; if we still
    // don't receive credentials, surface a clear error with the raw response for diagnostics.
    if (response == null || response.credentials() == null) {
      LOG.warn(
          "AssumeRole returned empty credentials (response={}). Attempting one retry with AWS arn prefix.",
          response);
      final String fallbackPolicyJson =
          policyString("aws", allowListOperation, allowedReadLocations, allowedWriteLocations)
              .toJson();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrying AssumeRole with fallback policy: {}", fallbackPolicyJson);
      }
      AssumeRoleRequest retryRequest = request.policy(fallbackPolicyJson).build();
      response = stsClient.assumeRole(retryRequest);
      // If SDK still didn't populate credentials, attempt to parse the raw XML captured
      // by the StsClientsPool debug interceptor (best-effort). This helps with some ECS
      // on-prem STS endpoints that return non-standard xmlns values that confuse the SDK.
      if (response == null || response.credentials() == null) {
        try {
          java.util.Optional<String> rawOpt = stsClientProvider.lastRawBody();
          if (rawOpt.isPresent()) {
            String raw = rawOpt.get();
            EcsXmlParser.Credentials creds = EcsXmlParser.parse(raw);
            if (creds != null) {
              // Build a synthetic AssumeRoleResponse-like mapping by directly filling AccessConfig
              AccessConfig.Builder accessConfigFallback = AccessConfig.builder();
              accessConfigFallback.put(StorageAccessProperty.AWS_KEY_ID, creds.getAccessKeyId());
              accessConfigFallback.put(
                  StorageAccessProperty.AWS_SECRET_KEY, creds.getSecretAccessKey());
              accessConfigFallback.put(StorageAccessProperty.AWS_TOKEN, creds.getSessionToken());
              if (creds.getExpiration() != null) {
                try {
                  long epoch = java.time.Instant.parse(creds.getExpiration()).toEpochMilli();
                  accessConfigFallback.put(
                      StorageAccessProperty.EXPIRATION_TIME, String.valueOf(epoch));
                  accessConfigFallback.put(
                      StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS, String.valueOf(epoch));
                } catch (Exception e) {
                  // If parsing fails, skip expiration fields
                }
              }
              // Copy metadata from the storage config into the fallback AccessConfig so
              // downstream consumers (e.g. DefaultFileIOFactory -> Iceberg S3FileIO) can
              // honor endpoint overrides, path-style access, region, and refresh endpoints.
              if (region != null) {
                accessConfigFallback.put(StorageAccessProperty.CLIENT_REGION, region);
              }

              refreshCredentialsEndpoint.ifPresent(
                  endpoint ->
                      accessConfigFallback.put(
                          StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint));

              URI fbEndpointUri = storageConfig.getEndpointUri();
              if (fbEndpointUri != null) {
                accessConfigFallback.put(
                    StorageAccessProperty.AWS_ENDPOINT, fbEndpointUri.toString());
              }
              URI fbInternalEndpointUri = storageConfig.getInternalEndpointUri();
              if (fbInternalEndpointUri != null) {
                accessConfigFallback.putInternalProperty(
                    StorageAccessProperty.AWS_ENDPOINT.getPropertyName(),
                    fbInternalEndpointUri.toString());
              }

              if (Boolean.TRUE.equals(storageConfig.getPathStyleAccess())) {
                accessConfigFallback.put(
                    StorageAccessProperty.AWS_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
              }

              LOG.info("Successfully parsed ECS STS XML fallback and extracted credentials");
              return accessConfigFallback.build();
            }
          }
        } catch (Exception e) {
          LOG.debug("ECS XML fallback parse failed", e);
        }
      }
      if (response == null || response.credentials() == null) {
        String respStr = response == null ? "null" : response.toString();
        LOG.error("AssumeRole retry did not return credentials. response={}", respStr);
        throw new UnprocessableEntityException("Failed to get subscoped credentials: %s", respStr);
      }

      accessConfig.put(StorageAccessProperty.AWS_KEY_ID, response.credentials().accessKeyId());
    accessConfig.put(
        StorageAccessProperty.AWS_SECRET_KEY, response.credentials().secretAccessKey());
    accessConfig.put(StorageAccessProperty.AWS_TOKEN, response.credentials().sessionToken());
    Optional.ofNullable(response.credentials().expiration())
        .ifPresent(
            i -> {
              accessConfig.put(
                  StorageAccessProperty.EXPIRATION_TIME, String.valueOf(i.toEpochMilli()));
              accessConfig.put(
                  StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS,
                  String.valueOf(i.toEpochMilli()));
            });

    if (region != null) {
      accessConfig.put(StorageAccessProperty.CLIENT_REGION, region);
    }

    refreshCredentialsEndpoint.ifPresent(
        endpoint -> {
          accessConfig.put(StorageAccessProperty.AWS_REFRESH_CREDENTIALS_ENDPOINT, endpoint);
        });

    URI endpointUri = storageConfig.getEndpointUri();
    if (endpointUri != null) {
      accessConfig.put(StorageAccessProperty.AWS_ENDPOINT, endpointUri.toString());
    }
    URI internalEndpointUri = storageConfig.getInternalEndpointUri();
    if (internalEndpointUri != null) {
      accessConfig.putInternalProperty(
          StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), internalEndpointUri.toString());
    }

      if (Boolean.TRUE.equals(storageConfig.getPathStyleAccess())) {
        accessConfig.put(StorageAccessProperty.AWS_PATH_STYLE_ACCESS, Boolean.TRUE.toString());
      }

      if ("aws-us-gov".equals(storageConfig.getAwsPartition()) && region == null) {
        throw new IllegalArgumentException(
            String.format(
                "AWS region must be set when using partition %s", storageConfig.getAwsPartition()));
      }
    }
    }

    return accessConfig.build();
  }

  private boolean shouldUseSts(AwsStorageConfigurationInfo storageConfig) {
    return !Boolean.TRUE.equals(storageConfig.getStsUnavailable());
  }

  /**
   * generate an IamPolicy from the input readLocations and writeLocations, optionally with list
   * support. Credentials will be scoped to exactly the resources provided. If read and write
   * locations are empty, a non-empty policy will be generated that grants GetObject and optionally
   * ListBucket privileges with no resources. This prevents us from sending an empty policy to AWS
   * and just assuming the role with full privileges.
   */
  // TODO - add KMS key access
  private IamPolicy policyString(
      String awsPartition,
      boolean allowList,
      Set<String> readLocations,
      Set<String> writeLocations) {
    IamPolicy.Builder policyBuilder = IamPolicy.builder();
    IamStatement.Builder allowGetObjectStatementBuilder =
        IamStatement.builder()
            .effect(IamEffect.ALLOW)
            .addAction("s3:GetObject")
            .addAction("s3:GetObjectVersion");
    Map<String, IamStatement.Builder> bucketListStatementBuilder = new HashMap<>();
    Map<String, IamStatement.Builder> bucketGetLocationStatementBuilder = new HashMap<>();

    final String arnPrefix = arnPrefixForPartition(awsPartition);
    // For S3-compatible providers that return partition 'ecs', the policy
    // resources should use the AWS-style S3 ARN prefix (arn:aws:s3:::). For
    // real AWS partitions (aws, aws-us-gov, aws-cn) use the configured
    // partition's prefix. This preserves deterministic policy output while
    // allowing ECS to receive AWS-style ARNs as expected by tests and some
    // S3-compatible endpoints.
    final String preferredArnPrefix =
        "ecs".equals(awsPartition) ? arnPrefixForPartition("aws") : arnPrefix;
    Stream.concat(readLocations.stream(), writeLocations.stream())
        .distinct()
        .forEach(
            location -> {
              URI uri = URI.create(location);
              // add only object-level resource using the configured partition prefix.
              allowGetObjectStatementBuilder.addResource(
                  IamResource.create(
                      preferredArnPrefix
                          + StorageUtil.concatFilePrefixes(parseS3Path(uri), "*", "/")));
              final var bucket = preferredArnPrefix + StorageUtil.getBucket(uri);
              if (allowList) {
                bucketListStatementBuilder
                    .computeIfAbsent(
                        bucket,
                        (String key) ->
                            IamStatement.builder()
                                .effect(IamEffect.ALLOW)
                                .addAction("s3:ListBucket")
                                .addResource(key))
                    .addCondition(
                        IamConditionOperator.STRING_LIKE,
                        "s3:prefix",
                        StorageUtil.concatFilePrefixes(trimLeadingSlash(uri.getPath()), "*", "/"));
              }
              bucketGetLocationStatementBuilder.computeIfAbsent(
                  bucket,
                  key ->
                      IamStatement.builder()
                          .effect(IamEffect.ALLOW)
                          .addAction("s3:GetBucketLocation")
                          .addResource(key));
            });

    if (!writeLocations.isEmpty()) {
      IamStatement.Builder allowPutObjectStatementBuilder =
          IamStatement.builder()
              .effect(IamEffect.ALLOW)
              .addAction("s3:PutObject")
              .addAction("s3:DeleteObject");
      writeLocations.forEach(
          location -> {
            URI uri = URI.create(location);
            // add object-level resources for write operations only using configured
            // partition prefix.
            allowPutObjectStatementBuilder.addResource(
                IamResource.create(
                    preferredArnPrefix
                        + StorageUtil.concatFilePrefixes(parseS3Path(uri), "*", "/")));
          });
      policyBuilder.addStatement(allowPutObjectStatementBuilder.build());
    }
    if (!bucketListStatementBuilder.isEmpty()) {
      bucketListStatementBuilder
          .values()
          .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    } else if (allowList) {
      // add list privilege with 0 resources
      policyBuilder.addStatement(
          IamStatement.builder().effect(IamEffect.ALLOW).addAction("s3:ListBucket").build());
    }

    bucketGetLocationStatementBuilder
        .values()
        .forEach(statementBuilder -> policyBuilder.addStatement(statementBuilder.build()));
    return policyBuilder.addStatement(allowGetObjectStatementBuilder.build()).build();
  }

  private static String arnPrefixForPartition(String awsPartition) {
    return String.format("arn:%s:s3:::", awsPartition != null ? awsPartition : "aws");
  }

  private static @Nonnull String parseS3Path(URI uri) {
    String bucket = StorageUtil.getBucket(uri);
    String path = trimLeadingSlash(uri.getPath());
    return String.join("/", bucket, path);
  }

  private static @Nonnull String trimLeadingSlash(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return path;
  }
}

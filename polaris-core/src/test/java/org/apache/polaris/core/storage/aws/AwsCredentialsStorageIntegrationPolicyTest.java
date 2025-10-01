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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;

public class AwsCredentialsStorageIntegrationPolicyTest {

  // simple stub StsClientProvider that won't be used by the test
  private static final StsClientProvider DUMMY_PROVIDER = destination -> (StsClient) null;

  @Test
  public void policyShouldContainBucketAndObjectArns_forEcsPartition()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // AwsStorageConfigurationInfo is abstract; create a minimal anonymous subclass for test
    AwsStorageConfigurationInfo cfg =
        new AwsStorageConfigurationInfo() {
          @Override
          public String getRoleARN() {
            return null;
          }

          @Override
          public String getExternalId() {
            return null;
          }

          @Override
          public String getUserARN() {
            return null;
          }

          @Override
          public String getRegion() {
            return null;
          }

          @Override
          public String getEndpoint() {
            return null;
          }

          @Override
          public String getEndpointInternal() {
            return null;
          }

          @Override
          public Boolean getPathStyleAccess() {
            return Boolean.TRUE;
          }

          @Override
          public String getStsEndpoint() {
            return null;
          }

          @Override
          public Boolean getIgnoreSSLVerification() {
            return Boolean.TRUE;
          }

          @Override
          public java.util.List<String> getAllowedLocations() {
            return java.util.Collections.emptyList();
          }

          @Override
          public Boolean getStsUnavailable() {
            return null;
          }
        };
    AwsCredentialsStorageIntegration integ =
        new AwsCredentialsStorageIntegration(cfg, DUMMY_PROVIDER);

    Method m =
        AwsCredentialsStorageIntegration.class.getDeclaredMethod(
            "policyString", String.class, boolean.class, Set.class, Set.class);
    m.setAccessible(true);

    Set<String> read = Collections.emptySet();
    Set<String> write = Collections.singleton("s3://bucketname/namespace/table01");

    Object iamPolicy = m.invoke(integ, "ecs", Boolean.FALSE, read, write);
    Method toJson = iamPolicy.getClass().getMethod("toJson");
    String json = (String) toJson.invoke(iamPolicy);

    assertTrue(json.contains("arn:aws:s3:::"), "policy should contain ecs arn prefix");
    assertTrue(json.contains("arn:aws:s3:::bucketname"), "policy should contain bucket ARN");
    assertTrue(
        json.contains("arn:aws:s3:::bucketname/namespace/table01/*"),
        "policy should contain object ARN with wildcard");
  }
}

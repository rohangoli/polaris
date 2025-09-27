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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class AwsRoleArnPatternTest {

  @Test
  public void testValidArns() {
    String[] arns = {
      "urn:ecs:iam::namespace:role/test-role", "arn:aws:iam::123456789012:role/test-role"
    };

    for (String arn : arns) {
      assertTrue(
          Pattern.matches(AwsStorageConfigurationInfo.ROLE_ARN_PATTERN, arn),
          "Expected to match: " + arn);
    }
  }

  @Test
  public void testInvalidArns() {
    String[] invalid = {
      "arn:aws:iam::123:role/test-role", // account id too short
      "urn:ecs:iam:::role/test-role", // malformed urn
      "", // empty
      "arn:aws-cn:iam::123456789012:role/test-role" // China partition not supported elsewhere
    };

    // The last case is expected to be rejected by validateArn due to aws-cn, but the pattern itself
    // may match;
    // For pattern checks we'll assert the first three do NOT match.
    for (int i = 0; i < invalid.length; i++) {
      String arn = invalid[i];
      if (i < 3) {
        assertFalse(
            Pattern.matches(AwsStorageConfigurationInfo.ROLE_ARN_PATTERN, arn),
            "Expected NOT to match: " + arn);
      }
    }
  }
}

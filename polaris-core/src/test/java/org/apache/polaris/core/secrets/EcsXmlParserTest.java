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
package org.apache.polaris.core.secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class EcsXmlParserTest {

  @Test
  public void testParseSampleEcsXml() throws Exception {
    String ecsXml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            + "<ns2:AssumeRoleResponse xmlns:ns2=\"none\">"
            + "<ResponseMetadata><RequestId>0a00078d:196f956a4f3:42b38:0-none</RequestId></ResponseMetadata>"
            + "<AssumeRoleResult>"
            + "<AssumedRoleUser>"
            + "<Arn>urn:ecs:sts::otf_dev:assumed-role/assumeSameAccountOTF/PolarisAwsCredentialsStorageIntegration</Arn>"
            + "<AssumedRoleId>AROAE95F8953C905D8A1:PolarisAwsCredentialsStorageIntegration</AssumedRoleId>"
            + "</AssumedRoleUser>"
            + "<Credentials>"
            + "<AccessKeyId>ASIABECF84EC09E48BA5</AccessKeyId>"
            + "<Expiration>2025-09-27T08:05:43Z</Expiration>"
            + "<SecretAccessKey>CKYV6QhVpXgdmySX_ni4eK0OrYrakPqJQfq0Kbe2U1U</SecretAccessKey>"
            + "<SessionToken>test-session-token</SessionToken>"
            + "</Credentials>"
            + "<PackedPolicySize>1371</PackedPolicySize>"
            + "</AssumeRoleResult>"
            + "</ns2:AssumeRoleResponse>";

    EcsXmlParser.Credentials creds = EcsXmlParser.parse(ecsXml);
    assertNotNull(creds, "Credentials should not be null");
    assertEquals("ASIABECF84EC09E48BA5", creds.getAccessKeyId());
    assertEquals("CKYV6QhVpXgdmySX_ni4eK0OrYrakPqJQfq0Kbe2U1U", creds.getSecretAccessKey());
    assertEquals("test-session-token", creds.getSessionToken());
    assertEquals("2025-09-27T08:05:43Z", creds.getExpiration());
  }
}

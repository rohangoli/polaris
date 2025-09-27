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
package org.apache.polaris.service.storage.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;

class RawStsBodyCaptureInterceptorTest {

  @AfterEach
  void tearDown() {
    // Clear thread-local raw body
    try {
      org.apache.polaris.service.storage.aws.StsClientsPool.clearLastRawStsBody();
    } catch (Throwable ignored) {
    }
  }

  @Test
  void modifyHttpResponseContent_shouldCaptureAndReturnFreshStream() throws Exception {
    String xml =
        "<AssumeRoleResponse><Credentials><AccessKeyId>AKIA</AccessKeyId></Credentials></AssumeRoleResponse>";
    byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
    InputStream original = new ByteArrayInputStream(bytes);

    Context.ModifyHttpResponse ctx = mock(Context.ModifyHttpResponse.class);
    when(ctx.responseBody()).thenReturn(Optional.of(original));

    org.apache.polaris.service.storage.aws.StsClientsPool.RawStsBodyCaptureInterceptor interceptor =
        new org.apache.polaris.service.storage.aws.StsClientsPool.RawStsBodyCaptureInterceptor();
    ExecutionAttributes attrs = new ExecutionAttributes();

    Optional<InputStream> returned = interceptor.modifyHttpResponseContent(ctx, attrs);
    assertTrue(returned.isPresent(), "Returned optional should be present");

    // The thread-local provider should contain the raw XML captured by the interceptor
    String last = org.apache.polaris.service.storage.aws.StsClientsPool.getLastRawStsBody();
    assertNotNull(last, "Captured raw body should be present");
    assertEquals(xml, last, "Captured raw body should contain the exact raw XML");

    // The returned stream should contain the same bytes
    InputStream returnedIs = returned.get();
    byte[] returnedBytes = returnedIs.readAllBytes();
    assertArrayEquals(bytes, returnedBytes, "Returned stream must contain same bytes");
  }
}

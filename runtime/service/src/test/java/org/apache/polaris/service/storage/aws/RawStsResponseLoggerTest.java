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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;

/** Unit tests for the RawStsResponseLogger behaviour (non-destructive capture). */
public class RawStsResponseLoggerTest {

  public static final String SAMPLE =
      "<?xml version=\"1.0\"?><ns2:AssumeRoleResponse xmlns:ns2=\"none\">...</ns2:AssumeRoleResponse>";

  @Test
  public void testNonDestructiveCaptureUsesAsUtf8String() throws Exception {
    // Create a fake response object that exposes asUtf8String()
    @SuppressWarnings("unused")
    class Resp {
      public String asUtf8String() {
        return SAMPLE;
      }
    }

    Object respObj = new Resp();

    // Mock a SdkHttpFullResponse and have content() return our object
    software.amazon.awssdk.http.SdkHttpFullResponse sdkResp =
        mock(software.amazon.awssdk.http.SdkHttpFullResponse.class);
    // Use doReturn to avoid compile-time generic mismatch for Optional<AbortableInputStream>
    Mockito.doReturn(Optional.of(respObj)).when(sdkResp).content();

    Context.AfterTransmission ctx = mock(Context.AfterTransmission.class);
    when(ctx.httpResponse()).thenReturn(sdkResp);

    // RawStsResponseLogger is a private static nested class; instantiate reflectively
    Class<?> clazz =
        Class.forName("org.apache.polaris.service.storage.aws.StsClientsPool$RawStsResponseLogger");
    java.lang.reflect.Constructor<?> ctor = clazz.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object logger = ctor.newInstance();
    java.lang.reflect.Method m =
        clazz.getDeclaredMethod(
            "afterTransmission", Context.AfterTransmission.class, ExecutionAttributes.class);
    m.setAccessible(true);
    m.invoke(logger, ctx, new ExecutionAttributes());

    String last = StsClientsPool.getLastRawStsBody();
    assertThat(last).isNotNull();
    assertThat(last).contains("AssumeRoleResponse");
  }
}

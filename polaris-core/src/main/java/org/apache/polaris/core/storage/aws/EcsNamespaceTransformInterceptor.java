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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.utils.IoUtils;

/**
 * An {@link ExecutionInterceptor} that allows to modify the HTTP request and response. It is used
 * to transform the request and response to be compatible with ECS.
 */
public class EcsNamespaceTransformInterceptor implements ExecutionInterceptor {
  private final boolean isEcs;

  public EcsNamespaceTransformInterceptor(boolean isEcs) {
    this.isEcs = isEcs;
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    if (!isEcs) {
      return context.httpRequest();
    }

    SdkHttpRequest request = context.httpRequest();
    if (request instanceof SdkHttpFullRequest) {
      SdkHttpFullRequest fullRequest = (SdkHttpFullRequest) request;
      String existingPath = fullRequest.encodedPath();
      if (existingPath == null) {
        existingPath = "";
      }
      // Avoid double-prefixing when the SDK or other interceptors already set /sts
      if (existingPath.startsWith("/sts")) {
        return fullRequest;
      } else {
        return fullRequest.toBuilder().encodedPath("/sts" + existingPath).build();
      }
    }
    return request;
  }

  @Override
  public Optional<InputStream> modifyHttpResponseContent(
      Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
    if (!isEcs) {
      return context.responseBody();
    }
    // For ECS, we need to transform the XML namespace to be compatible with the AWS SDK
    return context
        .responseBody()
        .map(
            is -> {
              try {
                String originalXml = IoUtils.toUtf8String(is);
                String transformedXml =
                    originalXml.replaceAll(
                        "xmlns:ns2=\"none\"",
                        "xmlns:ns2=\"https://sts.amazonaws.com/doc/2011-06-15/\"");
                return new ByteArrayInputStream(transformedXml.getBytes(StandardCharsets.UTF_8));
              } catch (IOException e) {
                throw new RuntimeException("Error transforming ECS XML response", e);
              }
            });
  }
}

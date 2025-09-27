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

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.polaris.core.storage.aws.EcsNamespaceTransformInterceptor;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/** Minimal, clean StsClientsPool with a debugging interceptor. */
public class StsClientsPool implements StsClientProvider {
  private static final String CACHE_NAME = "sts-clients";

  private final Cache<StsClientProvider.StsDestination, StsClient> clients;
  private final Function<StsClientProvider.StsDestination, StsClient> clientBuilder;

  public StsClientsPool(
      int clientsCacheMaxSize, SdkHttpClient sdkHttpClient, MeterRegistry meterRegistry) {
    this(
        clientsCacheMaxSize,
        key -> defaultStsClient(key, sdkHttpClient),
        Optional.of(meterRegistry));
  }

  /**
   * Public accessor to retrieve the last raw STS response body captured by the debug interceptor
   * for the current thread. May be null if not available.
   */
  public static String getLastRawStsBody() {
    return RawStsResponseLogger.getLastRawBody();
  }

  public StsClientsPool(
      int clientsCacheMaxSize,
      SdkHttpClient sdkHttpClient,
      SdkHttpClient insecureHttpClient,
      MeterRegistry meterRegistry) {
    this(
        clientsCacheMaxSize,
        key -> createStsClient(key, sdkHttpClient, insecureHttpClient),
        Optional.of(meterRegistry));
  }

  @VisibleForTesting
  StsClientsPool(
      int maxSize,
      Function<StsClientProvider.StsDestination, StsClient> clientBuilder,
      Optional<MeterRegistry> meterRegistry) {
    this.clientBuilder = clientBuilder;
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .recordStats(() -> statsCounter(meterRegistry, maxSize))
            .build();
  }

  @Override
  public StsClient stsClient(StsClientProvider.StsDestination destination) {
    return clients.get(destination, clientBuilder);
  }

  @Override
  public java.util.Optional<String> lastRawBody() {
    String s = RawStsResponseLogger.getLastRawBody();
    return java.util.Optional.ofNullable(s);
  }

  /** Test helper to clear the last captured raw STS body for the current thread. */
  public static void clearLastRawStsBody() {
    RawStsResponseLogger.clearLastRawBody();
  }

  private static StsClient defaultStsClient(
      StsClientProvider.StsDestination parameters, SdkHttpClient sdkClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.overrideConfiguration(
        c -> {
          c.addExecutionInterceptor(new RawStsResponseLogger());
          c.addExecutionInterceptor(new RawStsBodyCaptureInterceptor());
          // If endpoint appears to be an ECS on-prem STS endpoint, register the namespace
          // transform interceptor so responses are adjusted to what the AWS SDK expects.
          if (parameters.endpoint().isPresent()
              && parameters.endpoint().get().toString().toLowerCase(Locale.ROOT).contains("ecs")) {
            c.addExecutionInterceptor(new EcsNamespaceTransformInterceptor(true));
          }
        });
    builder.httpClient(sdkClient);
    if (parameters.endpoint().isPresent()) {
      CompletableFuture<Endpoint> endpointFuture =
          completedFuture(Endpoint.builder().url(parameters.endpoint().get()).build());
      builder.endpointProvider(params -> endpointFuture);
    }

    parameters.region().ifPresent(r -> builder.region(Region.of(r)));

    return builder.build();
  }

  private static StsClient createStsClient(
      StsClientProvider.StsDestination parameters,
      SdkHttpClient secureClient,
      SdkHttpClient insecureClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.overrideConfiguration(
        c -> {
          c.addExecutionInterceptor(new RawStsResponseLogger());
          c.addExecutionInterceptor(new RawStsBodyCaptureInterceptor());
          if (parameters.endpoint().isPresent()
              && parameters.endpoint().get().toString().toLowerCase(Locale.ROOT).contains("ecs")) {
            c.addExecutionInterceptor(new EcsNamespaceTransformInterceptor(true));
          }
        });

    boolean ignoreSSL = parameters.ignoreSSLVerification().orElse(false);
    builder.httpClient(ignoreSSL ? insecureClient : secureClient);

    if (parameters.endpoint().isPresent()) {
      CompletableFuture<Endpoint> endpointFuture =
          completedFuture(Endpoint.builder().url(parameters.endpoint().get()).build());
      builder.endpointProvider(params -> endpointFuture);
    }

    parameters.region().ifPresent(r -> builder.region(Region.of(r)));

    return builder.build();
  }

  static StatsCounter statsCounter(Optional<MeterRegistry> meterRegistry, int maxSize) {
    if (meterRegistry.isPresent()) {
      meterRegistry
          .get()
          .gauge("max_entries", singletonList(Tag.of("cache", CACHE_NAME)), "", x -> maxSize);

      return new CaffeineStatsCounter(meterRegistry.get(), CACHE_NAME);
    }
    return StatsCounter.disabledStatsCounter();
  }

  /** ExecutionInterceptor that logs the SDK HTTP response representation after transmission. */
  private static final class RawStsResponseLogger implements ExecutionInterceptor {
    private static final org.slf4j.Logger LOG =
        org.slf4j.LoggerFactory.getLogger(StsClientsPool.class);
    // Store last raw STS body for diagnostic fallback (best-effort, non-destructive)
    private static final ThreadLocal<String> LAST_RAW_BODY = new ThreadLocal<>();

    @Override
    public void afterTransmission(
        Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
      try {
        Object httpResp = null;
        try {
          httpResp = context.httpResponse();
        } catch (Throwable ignored) {
        }
        // Log raw HTTP response representation at TRACE level only.
        if (httpResp != null) {
          try {
            // Best-effort attempt to obtain a textual representation without consuming
            // or closing the underlying InputStream. Avoid reading InputStream directly.
            String body = extractBodyAsString(httpResp);
            if (body != null) {
              LAST_RAW_BODY.set(body);
              // Log the raw body if available
              LOG.trace("Raw STS HTTP response body: {}", body);
            }
          } catch (Throwable t) {
            LOG.trace("Failed to extract STS response body via reflection", t);
          }
        }
      } catch (Exception e) {
        LOG.trace("Failed to log raw STS HTTP response", e);
      }
    }

    private static String extractBodyAsString(Object httpResp) {
      if (httpResp == null) return null;
      try {
        if (httpResp instanceof SdkHttpFullResponse resp) {
          try {
            Optional<?> content = resp.content();
            if (content.isPresent()) {
              Object c = content.get();
              String s = tryConvertToString(c);
              if (s != null) return s;
            }
          } catch (Throwable ignored) {
            // fall through to reflection
          }
        }

        for (java.lang.reflect.Method m : httpResp.getClass().getMethods()) {
          String name = m.getName().toLowerCase(Locale.ROOT);
          if (!name.contains("content")
              && !name.contains("body")
              && !name.contains("payload")
              && !name.contains("stream")) {
            continue;
          }
          try {
            Object val = m.invoke(httpResp);
            Object resolved = resolveOptional(val);
            if (resolved == null) continue;
            String s = tryConvertToString(resolved);
            if (s != null) return s;
          } catch (Throwable ignored) {
            // try next
          }
        }
      } catch (Throwable t) {
        LOG.trace("Error extracting STS http body", t);
      }
      return null;
    }

    private static Object resolveOptional(Object maybeOptional) {
      if (maybeOptional == null) return null;
      try {
        if (maybeOptional instanceof Optional) {
          return ((Optional<?>) maybeOptional).orElse(null);
        }
        return maybeOptional;
      } catch (Throwable t) {
        return maybeOptional;
      }
    }

    private static String tryConvertToString(Object resolved) {
      if (resolved == null) return null;
      try {
        if (resolved instanceof String s) return s;
        if (resolved instanceof byte[] arr) return new String(arr, StandardCharsets.UTF_8);
        if (resolved instanceof ByteBuffer bb) {
          ByteBuffer copy = bb.asReadOnlyBuffer();
          byte[] arr = new byte[copy.remaining()];
          copy.get(arr);
          return new String(arr, StandardCharsets.UTF_8);
        }
        // Do NOT read InputStream here; doing so will consume/close it and
        // prevent the AWS SDK from parsing the response. Return null and
        // allow other non-destructive paths to be attempted.
        if (resolved instanceof InputStream) return null;
        try {
          java.lang.reflect.Method asBytes = resolved.getClass().getMethod("asByteArray");
          Object b = asBytes.invoke(resolved);
          if (b instanceof byte[] array) return new String(array, StandardCharsets.UTF_8);
        } catch (NoSuchMethodException ignored) {
        }
        try {
          java.lang.reflect.Method asUtf = resolved.getClass().getMethod("asUtf8String");
          Object s = asUtf.invoke(resolved);
          if (s instanceof String string) return string;
        } catch (NoSuchMethodException ignored) {
        }
        String t = resolved.toString();
        return t == null ? null : t;
      } catch (Throwable t) {
        return null;
      }
    }

    static String getLastRawBody() {
      return LAST_RAW_BODY.get();
    }

    static void clearLastRawBody() {
      LAST_RAW_BODY.remove();
    }

    static void setLastRawBody(String body) {
      LAST_RAW_BODY.set(body);
    }
  }

  /**
   * ExecutionInterceptor that captures the raw HTTP response body (best-effort) by reading and
   * duplicating the response InputStream. The duplicated bytes are stored in a thread-local so
   * higher-level fallback parsers and tests can inspect the raw STS XML when the SDK fails to
   * populate credentials.
   */
  static final class RawStsBodyCaptureInterceptor implements ExecutionInterceptor {
    @Override
    public Optional<InputStream> modifyHttpResponseContent(
        Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
      try {
        Optional<InputStream> bodyOpt = context.responseBody();
        if (bodyOpt != null && bodyOpt.isPresent()) {
          InputStream is = bodyOpt.get();
          byte[] bytes = is.readAllBytes();
          if (bytes != null && bytes.length > 0) {
            try {
              String raw = new String(bytes, StandardCharsets.UTF_8);
              // Store in thread-local so the provider API and tests can access it
              RawStsResponseLogger.setLastRawBody(raw);
            } catch (Throwable ignored) {
              // ignore
            }
          }
          // Return a fresh stream with the same bytes so the SDK can still parse it
          return Optional.of(new java.io.ByteArrayInputStream(bytes));
        }
      } catch (Throwable t) {
        // best-effort — do not interfere with normal SDK processing
      }
      return context.responseBody();
    }
  }
}

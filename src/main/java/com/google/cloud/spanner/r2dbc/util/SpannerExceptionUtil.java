/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc.util;

import com.google.rpc.RetryInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import java.time.Duration;

/**
 * Extracts metadata from exceptions thrown during Spanner RPCs.
 */
public class SpannerExceptionUtil {
  private static final Metadata.Key<RetryInfo> KEY_RETRY_INFO =
      ProtoUtils.keyForProto(RetryInfo.getDefaultInstance());

  /**
   * Returns whether an exception thrown should be retried.
   *
   * <p>Derived from google-cloud-java/SpannerExceptionFactory.java:
   * https://github.com/googleapis/google-cloud-java/blob/master/google-cloud-clients/google-cloud-spanner/src/main/java/com/google/cloud/spanner/SpannerExceptionFactory.java
   */
  static boolean isRetryable(Throwable cause) {
    if (cause instanceof StatusRuntimeException) {
      StatusRuntimeException statusRuntimeException = (StatusRuntimeException) cause;

      if (statusRuntimeException.getStatus().getCode() == Status.Code.INTERNAL) {
        if (cause.getMessage().contains("HTTP/2 error code: INTERNAL_ERROR")) {
          return true;
        }
        if (cause.getMessage().contains("Connection closed with unknown cause")) {
          return true;
        }
        if (cause
            .getMessage()
            .contains("Received unexpected EOS on DATA frame from server")) {
          return true;
        }
      }

      if (statusRuntimeException.getStatus().getCode() == Code.RESOURCE_EXHAUSTED
          && extractRetryDelay(statusRuntimeException).toMillis() > 0L) {
        return true;
      }
    }

    return false;
  }

  /**
   * Extracts the retry delay from the Spanner exception if it exists; else returns Duration
   * of -1 seconds.
   */
  private static Duration extractRetryDelay(Throwable cause) {
    Metadata trailers = Status.trailersFromThrowable(cause);
    if (trailers != null && trailers.containsKey(KEY_RETRY_INFO)) {
      RetryInfo retryInfo = trailers.get(KEY_RETRY_INFO);
      if (retryInfo.hasRetryDelay()) {
        com.google.protobuf.Duration protobufDuration = retryInfo.getRetryDelay();
        return Duration.ofSeconds(protobufDuration.getSeconds())
            .withNanos(protobufDuration.getNanos());
      }
    }

    return Duration.ofSeconds(-1);
  }
}

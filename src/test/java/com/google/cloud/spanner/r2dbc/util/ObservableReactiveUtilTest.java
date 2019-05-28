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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Test for {@link ObservableReactiveUtil}.
 */
public class ObservableReactiveUtilTest {

  @Test
  public void unaryCallReturnsSingleValue() {
    Mono<Integer> mono = ObservableReactiveUtil.unaryCall(observer -> {
      observer.onNext(42);
      observer.onCompleted();
    });
    assertThat(mono.block()).isEqualTo(42);
  }

  @Test
  public void unaryCallForwardsError() {
    Mono<Integer> mono = ObservableReactiveUtil.unaryCall(observer -> {
      observer.onError(new IllegalArgumentException("oh no"));
    });
    assertThatThrownBy(() -> mono.block())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("oh no");
  }

  @Test
  public void unaryCallThrowsExceptionIfCompletedWithNoValue() {
    Mono<Integer> mono = ObservableReactiveUtil.unaryCall(observer -> observer.onCompleted());
    assertThatThrownBy(() -> mono.block())
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Unary gRPC call completed without yielding a value or an error");
  }

  @Test
  public void propagateTransientErrorUnaryCall() {
    StatusRuntimeException retryableException =
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR"));

    Mono<Void> result =
        ObservableReactiveUtil.unaryCall(observer -> observer.onError(retryableException));

    assertThatThrownBy(() -> result.block())
        .isInstanceOf(R2dbcTransientResourceException.class);
  }

  @Test
  public void propagateNonRetryableError() {
    Mono<Void> result =
        ObservableReactiveUtil.unaryCall(
            observer -> observer.onError(new IllegalArgumentException()));

    assertThatThrownBy(() -> result.block())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void propagateTransientErrorStreamingCall() {
    StatusRuntimeException retryableException =
        new StatusRuntimeException(
            Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR"));

    Flux<Void> result =
        ObservableReactiveUtil.streamingCall(observer -> observer.onError(retryableException));

    assertThatThrownBy(() -> result.blockFirst())
        .isInstanceOf(R2dbcTransientResourceException.class);
  }
}

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

import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

/**
 * Converter from a gRPC async calls to Reactor primitives ({@link Mono}).
 */
public class ObservableReactiveUtil {

  /**
   * Invokes a lambda that in turn issues a remote call, directing the response to a {@link Mono}
   * stream.
   * @param remoteCall lambda capable of invoking the correct remote call, making use of the
   * {@link Mono}-converting {@link StreamObserver} implementation.
   * @param <ResponseT> type of remote call response
   * @return
   */
  public static <ResponseT> Mono<ResponseT> unaryCall(
      Consumer<StreamObserver<ResponseT>> remoteCall) {
    return Mono.create(sink -> {
      remoteCall.accept(new StreamObserver<ResponseT>() {
        @Override
        public void onNext(ResponseT response) {
          sink.success(response);
        }

        @Override
        public void onError(Throwable throwable) {
          sink.error(throwable);
        }

        @Override
        public void onCompleted() {
          // do nothing; Mono will already be completed by onNext/onError.
        }
      });
    });
  }
}

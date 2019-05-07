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

package com.google.cloud.spanner.r2dbc.client;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.r2dbc.util.ObservableReactiveUtil;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.SpannerGrpc.SpannerStub;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

/**
 * gRPC-based {@link Client} implementation.
 */
public class GrpcClient implements Client {

  public static final String HOST = "spanner.googleapis.com";
  public static final int PORT = 443;

  private SpannerStub spanner;

  /**
   * Creates the gRPC stub with authentication.
   * @throws IOException if credentials could not be acquired
   */
  public void initialize() throws IOException {
    // Create blocking and async stubs using the channel
    CallCredentials callCredentials = MoreCallCredentials
        .from(GoogleCredentials.getApplicationDefault());

    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress(HOST, PORT)
        .build();

    // Create the asynchronous stub for Cloud Spanner
    this.spanner = SpannerGrpc.newStub(channel)
        .withCallCredentials(callCredentials);
  }

  /**
   * Creates a Spanner session to be used for any future operations.
   * @param databaseName Fully qualified database name in {@code project/instance/database} format.
   * @return {@link Mono} of the created session.
   */
  public Mono<Session> createSession(String databaseName) {
    CreateSessionRequest request = CreateSessionRequest.newBuilder()
        .setDatabase(databaseName)
        .build();
    return ObservableReactiveUtil.unaryCall((obs) -> this.spanner.createSession(request, obs));
  }

  @Override
  public Mono<Void> close() {
    return null;
  }

  @Override
  public Publisher<PartialResultSet> executeStreamingSql(ExecuteSqlRequest request) {
    return subscriber -> spanner.executeStreamingSql(request,
        new ClientResponseObserver<ExecuteSqlRequest, PartialResultSet>() {
          @Override
          public void beforeStart(
              ClientCallStreamObserver<ExecuteSqlRequest> clientCallStreamObserver) {

            clientCallStreamObserver.disableAutoInboundFlowControl();

            subscriber.onSubscribe(new Subscription() {
              @Override
              public void request(long l) {
                clientCallStreamObserver.request((int) l);
              }

              @Override
              public void cancel() {
                clientCallStreamObserver.cancel(null, null);
              }
            });
          }

          @Override
          public void onNext(PartialResultSet partialResultSet) {
            subscriber.onNext(partialResultSet);
          }

          @Override
          public void onError(Throwable throwable) {
            subscriber.onError(throwable);
          }

          @Override
          public void onCompleted() {
            subscriber.onComplete();
          }
        });

  }
}

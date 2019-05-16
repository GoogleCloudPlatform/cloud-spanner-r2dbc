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
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.Value.KindCase;
import com.google.spanner.v1.BeginTransactionRequest;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.CreateSessionRequest;
import com.google.spanner.v1.DeleteSessionRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.RollbackRequest;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc;
import com.google.spanner.v1.SpannerGrpc.SpannerStub;
import com.google.spanner.v1.Transaction;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.ReadWrite;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * gRPC-based {@link Client} implementation.
 */
public class GrpcClient implements Client {

  public static final String HOST = "spanner.googleapis.com";
  public static final int PORT = 443;

  private final ManagedChannel channel;
  private final SpannerStub spanner;

  /**
   * Initializes the Cloud Spanner gRPC async stub.
   */
  public GrpcClient(GoogleCredentials credentials) {
    // Create blocking and async stubs using the channel
    CallCredentials callCredentials = MoreCallCredentials.from(credentials);

    // Create a channel
    this.channel = ManagedChannelBuilder
        .forAddress(HOST, PORT)
        .build();

    // Create the asynchronous stub for Cloud Spanner
    this.spanner = SpannerGrpc.newStub(this.channel)
        .withCallCredentials(callCredentials);
  }

  @VisibleForTesting
  GrpcClient(ManagedChannel channel, SpannerStub spanner) {
    this.channel = channel;
    this.spanner = spanner;
  }

  @Override
  public Mono<Transaction> beginTransaction(Session session) {
    return Mono.defer(() -> {
      BeginTransactionRequest beginTransactionRequest =
          BeginTransactionRequest.newBuilder()
              .setSession(session.getName())
              .setOptions(
                  TransactionOptions
                      .newBuilder()
                      .setReadWrite(ReadWrite.getDefaultInstance()))
              .build();

      return ObservableReactiveUtil.unaryCall(
          (obs) -> this.spanner.beginTransaction(beginTransactionRequest, obs));
    });
  }

  @Override
  public Mono<CommitResponse> commitTransaction(Session session, Transaction transaction) {
    return Mono.defer(() -> {
      CommitRequest commitRequest =
          CommitRequest.newBuilder()
              .setSession(session.getName())
              .setTransactionId(transaction.getId())
              .build();

      return ObservableReactiveUtil.unaryCall(
          (obs) -> this.spanner.commit(commitRequest, obs));
    });
  }

  @Override
  public Mono<Void> rollbackTransaction(Session session, Transaction transaction) {
    return Mono.defer(() -> {
      RollbackRequest rollbackRequest =
          RollbackRequest.newBuilder()
              .setSession(session.getName())
              .setTransactionId(transaction.getId())
              .build();

      return ObservableReactiveUtil.<Empty>unaryCall(
          (obs) -> this.spanner.rollback(rollbackRequest, obs))
          .then();
    });
  }

  @Override
  public Mono<Session> createSession(String databaseName) {
    return Mono.defer(() -> {
      CreateSessionRequest request = CreateSessionRequest.newBuilder()
          .setDatabase(databaseName)
          .build();

      return ObservableReactiveUtil.unaryCall((obs) -> this.spanner.createSession(request, obs));
    });
  }

  @Override
  public Mono<Void> deleteSession(Session session) {
    return Mono.defer(() -> {
      DeleteSessionRequest deleteSessionRequest =
          DeleteSessionRequest.newBuilder()
              .setName(session.getName())
              .build();

      return ObservableReactiveUtil.<Empty>unaryCall(
          (obs) -> this.spanner.deleteSession(deleteSessionRequest, obs))
          .then();
    });
  }

  @Override
  public Publisher<PartialResultSet> executeStreamingSql(ExecuteSqlRequest request) {
    return Flux.create(sink -> {
      ClientResponseObserver<ExecuteSqlRequest, PartialResultSet> clientResponseObserver =
          new ClientResponseObserver<ExecuteSqlRequest, PartialResultSet>() {
            @Override
            public void onNext(PartialResultSet value) {
              sink.next(value);
            }

            @Override
            public void onError(Throwable t) {
              sink.error(t);
            }

            @Override
            public void onCompleted() {
              sink.complete();
            }

            @Override
            public void beforeStart(ClientCallStreamObserver<ExecuteSqlRequest> requestStream) {
              requestStream.disableAutoInboundFlowControl();
              sink.onRequest(demand -> requestStream.request((int) demand));
              sink.onCancel(() -> requestStream.cancel(null, null));
            }
          };
      this.spanner.executeStreamingSql(request, clientResponseObserver);
    });
  }

  @Override
  public Tuple2<Mono<ResultSetMetadata>, Flux<List<Value>>> assembleRowsFromPartialResults(
      Publisher<PartialResultSet> partialResultSetPublisher) {
    Flux<PartialResultSet> partialResultSetFlux = Flux.from(partialResultSetPublisher);
    Mono<ResultSetMetadata> resultSetMetadataMono = partialResultSetFlux.next()
        .map(PartialResultSet::getMetadata);
    return Tuples.of(resultSetMetadataMono,
        streamingListValueRows(partialResultSetFlux, resultSetMetadataMono));
  }

  private Flux<List<Value>> streamingListValueRows(Flux<PartialResultSet> partialResultSetFlux,
      Mono<ResultSetMetadata> resultSetMetadataMono) {

    AtomicReference<
        Flux<Tuple2<ResultSetMetadata, PartialResultSet>>> zipped =
        new AtomicReference<>(
            Flux.combineLatest(resultSetMetadataMono, partialResultSetFlux, Tuples::of));

    return Flux.create(sink -> {
      AtomicBoolean prevIsChunk = new AtomicBoolean(false);
      AtomicReference<List<Value>> currentRow = new AtomicReference<>(new ArrayList<>());
      AtomicInteger rowSize = new AtomicInteger(-1);
      AtomicReference incompletePiece = new AtomicReference();
      AtomicReference<KindCase> incompletePieceKind = new AtomicReference<>();

      Consumer<Value> appendToRow = val -> {
        currentRow.get().add(val);
        if (currentRow.get().size() == rowSize.get()) {
          sink.next(currentRow.get());
          currentRow.set(new ArrayList<>());
        }
      };

      zipped.set(zipped.get().doOnNext(t -> {
        if (rowSize.get() == -1) {
          rowSize.set(t.getT1().getRowType().getFieldsCount());
        }
        PartialResultSet partialResultSet = t.getT2();
        int availableCount = partialResultSet.getValuesCount();

        if (prevIsChunk.get()) {
          Value firstPiece = partialResultSet.getValues(0);

          // Concat code from client lib
          if (incompletePieceKind.get() == KindCase.STRING_VALUE) {
            incompletePiece.set(incompletePiece.get() + firstPiece.getStringValue());
          } else {
            concatLists((List<Value>) incompletePiece.get(),
                firstPiece.getListValue().getValuesList());
          }
        }

        /* if there are more values then it means the incomplete piece is complete.
          Also, if this PR isn't chunked then it is also complete.
         */
        if (availableCount > 1 || !partialResultSet.getChunkedValue()) {
          if (prevIsChunk.get()) {
            appendToRow.accept(
                incompletePieceKind.get() == KindCase.STRING_VALUE
                    ? Value.newBuilder().setStringValue((String) incompletePiece.get()).build()
                    : Value.newBuilder()
                        .setListValue(
                            ListValue.newBuilder()
                                .addAllValues((List<Value>) incompletePiece.get()))
                        .build()
            );
            prevIsChunk.set(false);
          } else {
            appendToRow.accept(partialResultSet.getValues(0));
          }
        }

        /* Only the final value can be chunked, and only the first value can be
          a part of a previous chunk, so the pieces in the middle are always
          whole values.
        * */
        for (int i = 1; i < availableCount - 1; i++) {
          appendToRow.accept(partialResultSet.getValues(i));
        }

        // this final piece is the start of a new incomplete value
        if (!prevIsChunk.get() && partialResultSet.getChunkedValue()) {
          Value val = partialResultSet.getValues(availableCount - 1);
          incompletePieceKind.set(val.getKindCase());
          incompletePiece.set(val.getKindCase() == KindCase.STRING_VALUE ? val.getStringValue() :
              new ArrayList<>(val.getListValue().getValuesList()));
        }

        prevIsChunk.set(partialResultSet.getChunkedValue());
      }));

      zipped.set(zipped.get().doOnComplete(sink::complete));

      zipped.set(zipped.get().doOnError(sink::error));

      zipped.set(zipped.get().doOnTerminate(sink::complete));

      sink.onRequest(r -> zipped.get().subscribe());
    });
  }

  @Override
  public Mono<Void> close() {
    return Mono.fromRunnable(this.channel::shutdownNow);
  }

  // Client lib definition. These kind-cases are not mergeable for PartialResultSet.
  private boolean isMergeable(KindCase kind) {
    return kind == KindCase.STRING_VALUE || kind == KindCase.LIST_VALUE;
  }

  /**
   * Used to merge List-column value chunks. From Client lib.
   */
  private void concatLists(List<com.google.protobuf.Value> a, List<com.google.protobuf.Value> b) {
    if (a.size() == 0 || b.size() == 0) {
      a.addAll(b);
    } else {
      com.google.protobuf.Value last = a.get(a.size() - 1);
      com.google.protobuf.Value first = b.get(0);
      KindCase lastKind = last.getKindCase();
      KindCase firstKind = first.getKindCase();
      if (isMergeable(lastKind) && lastKind == firstKind) {
        com.google.protobuf.Value merged = null;
        if (lastKind == KindCase.STRING_VALUE) {
          String lastStr = last.getStringValue();
          String firstStr = first.getStringValue();
          merged =
              com.google.protobuf.Value.newBuilder().setStringValue(lastStr + firstStr).build();
        } else { // List
          List<Value> mergedList = new ArrayList<>(last.getListValue().getValuesList());
          concatLists(mergedList, first.getListValue().getValuesList());
          merged =
              com.google.protobuf.Value.newBuilder()
                  .setListValue(ListValue.newBuilder().addAllValues(mergedList))
                  .build();
        }
        a.set(a.size() - 1, merged);
        a.addAll(b.subList(1, b.size()));
      } else {
        a.addAll(b);
      }
    }
  }
}

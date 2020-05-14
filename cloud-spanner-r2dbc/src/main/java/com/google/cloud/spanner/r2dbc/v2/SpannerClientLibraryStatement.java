package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.AsyncResultSet;
import com.google.cloud.spanner.AsyncResultSet.CallbackResponse;
import com.google.cloud.spanner.AsyncResultSet.CursorState;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.SpannerException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class SpannerClientLibraryStatement implements Statement {

  // YOLO; very temporary. TODO: manage disposal.
  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private DatabaseClient databaseClient;

  private String query;

  public SpannerClientLibraryStatement(DatabaseClient databaseClient, String query) {
    this.databaseClient = databaseClient;
    this.query = query;
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(int index, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(String name, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<? extends Result> execute() {
    // TODO: unplaceholder singleUse, extract into member
    // make note -- timestamp bound passed here

    return Flux.create(sink -> {
      AsyncResultSet ars = this.databaseClient.singleUse().executeQueryAsync(
          com.google.cloud.spanner.Statement.of(this.query));
      sink.onCancel(ars::cancel);
      // TODO: handle backpressure
      //sink.onRequest()
      // TODO: elastic vs processor-bounded parallel
      ars.setCallback(this.executorService, rs -> this.callback(sink, rs) );
    });//.map(flux -> Mono.just(new R));
  }


  private CallbackResponse callback(FluxSink sink, AsyncResultSet resultSet) {
    try {
      // TODO: ask Knut if the infinit-ish loop is needed, given that callback is guaranteed
      // to be called again if we return CallbackResponse.CONTINUE (check that first)
      //while (true) {
      switch (resultSet.tryNext()) {
        case DONE:
          sink.complete();
          return CallbackResponse.DONE;
        case NOT_READY:
          return CallbackResponse.CONTINUE;
        case OK:
          // TODO un-null metadata
          // TODO: handle row count
          sink.next(new SpannerClientLibraryRow(resultSet.getCurrentRowAsStruct()));
          return CallbackResponse.CONTINUE;
      }
      //break;
    } catch (Throwable t) {
      sink.error(t);
      return CallbackResponse.DONE;
    }
  }
}

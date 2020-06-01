package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.r2dbc.SpannerRow;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SpannerClientLibraryResult implements Result {

  private final Flux<SpannerClientLibraryRow> resultRows;

  private final Mono<Integer> rowsUpdated;

  public SpannerClientLibraryResult(
      Flux<SpannerClientLibraryRow> resultRows, Mono<Integer> rowsUpdated) {
    this.resultRows = resultRows;
    this.rowsUpdated = rowsUpdated;
  }

  @Override
  public Publisher<Integer> getRowsUpdated() {
    return this.rowsUpdated;
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    if (this.resultRows == null) {
      return Flux.empty();
    }

    return this.resultRows.map(row -> mappingFunction.apply(row, /* TODO: row.getRowMetadata() */ null ));

  }
}

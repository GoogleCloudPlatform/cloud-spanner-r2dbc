package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.Struct;
import io.r2dbc.spi.Row;

public class SpannerClientLibraryRow implements Row {
  private Struct rowFields;

  public SpannerClientLibraryRow(Struct rowFields) {
    this.rowFields = rowFields;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    return null;
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    return null;
  }
}

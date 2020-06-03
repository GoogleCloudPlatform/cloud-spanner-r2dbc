package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.r2dbc.codecs.Codecs;
import com.google.cloud.spanner.r2dbc.codecs.DefaultCodecs;
import io.r2dbc.spi.Row;

public class SpannerClientLibraryRow implements Row {
  private Struct rowFields;

	private static final Codecs codecs = new DefaultCodecs();

	public SpannerClientLibraryRow(Struct rowFields) {
    this.rowFields = rowFields;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
	  return ClientLibraryCodec.decode(rowFields, index - 1, type);
  }

  @Override
  public <T> T get(String name, Class<T> type) {
	  return  ClientLibraryCodec.decode(rowFields, rowFields.getColumnIndex(name), type);
  }

}

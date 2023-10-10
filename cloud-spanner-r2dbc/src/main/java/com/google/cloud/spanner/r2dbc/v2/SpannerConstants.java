package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.TimestampBound;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;

public class SpannerConstants {
  public static Option<TimestampBound> TIMESTAMP_BOUND = Option.valueOf("timestampBound");

}

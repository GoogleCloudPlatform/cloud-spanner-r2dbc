package com.google.cloud.spanner.r2dbc.springdata;

import com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindMarkersFactoryResolver.BindMarkerFactoryProvider;

public class SpannerBindMarkerFactoryProvider implements BindMarkerFactoryProvider {

  @Override
  public BindMarkersFactory getBindMarkers(ConnectionFactory connectionFactory) {
    if (SpannerConnectionFactoryMetadata.INSTANCE.equals(connectionFactory.getMetadata())) {
      return SpannerR2dbcDialect.NAMED;
    }
    return null;
  }
}

package com.google.cloud.spanner.r2dbc.v2;

import io.r2dbc.spi.R2dbcNonTransientException;

public class BindingFailureException extends R2dbcNonTransientException {

  public BindingFailureException(String message) {
    super(message);
  }
}

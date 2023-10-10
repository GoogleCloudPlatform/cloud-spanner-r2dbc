package com.google.cloud.spanner.r2dbc.util;

import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import java.util.HashMap;
import java.util.Map;

public class TestTransactionDefinition implements TransactionDefinition {

  private final Map<Option<?>, Object> internalMap;

  TestTransactionDefinition(Map<Option<?>, Object> internalMap) {
    this.internalMap = internalMap;
  }

  @Override
  public <T> T getAttribute(Option<T> option) {
    return (T) this.internalMap.get(option);
  }


  public static class Builder {
    private final Map<Option<?>, Object> internalMap;

    public Builder() {
      this.internalMap = new HashMap<>();
    }

    public <T> Builder with(Option<T> option, T value) {
      this.internalMap.put(option, value);
      return this;
    }

    public TestTransactionDefinition build() {
      return new TestTransactionDefinition(internalMap);
    }
  }
}

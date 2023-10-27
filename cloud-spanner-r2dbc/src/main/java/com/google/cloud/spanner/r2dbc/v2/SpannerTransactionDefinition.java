/*
 * Copyright 2022-2023 Google LLC
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

package com.google.cloud.spanner.r2dbc.v2;

import static com.google.cloud.spanner.r2dbc.v2.SpannerConstants.TIMESTAMP_BOUND;
import static java.lang.Boolean.TRUE;

import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link TransactionDefinition} for Spanner Database.
 */
public class SpannerTransactionDefinition implements TransactionDefinition {

  private final Map<Option<?>, Object> internalMap;

  SpannerTransactionDefinition(Map<Option<?>, Object> internalMap) {
    validate(internalMap);
    this.internalMap = internalMap;
  }

  private void validate(Map<Option<?>, Object> internalMap) {
    boolean isReadOnlyTransaction = TRUE.equals(internalMap.get(READ_ONLY));
    if (!isReadOnlyTransaction && internalMap.containsKey(TIMESTAMP_BOUND)) {
      throw new IllegalArgumentException("TIMESTAMP_BOUND can only be configured for"
          + " read only transactions.");
    }
  }

  @Override
  public <T> T getAttribute(Option<T> option) {
    return (T) this.internalMap.get(option);
  }


  /**
   * A builder class for {@link SpannerTransactionDefinition}.
   */
  public static class Builder {
    private final Map<Option<?>, Object> internalMap;

    public Builder() {
      this.internalMap = new HashMap<>();
    }

    public <T> Builder with(Option<T> option, T value) {
      this.internalMap.put(option, value);
      return this;
    }

    public SpannerTransactionDefinition build() {
      return new SpannerTransactionDefinition(this.internalMap);
    }
  }
}

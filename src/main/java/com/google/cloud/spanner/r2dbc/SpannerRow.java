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

package com.google.cloud.spanner.r2dbc;

import com.google.cloud.spanner.Struct;
import com.google.common.annotations.VisibleForTesting;
import io.r2dbc.spi.Row;

/**
 * {@link Row} implementation for Cloud Spanner.
 *
 */
public class SpannerRow implements Row {

  private final Struct struct;

  /**
   * Constructor.
   *
   * @param struct the row from Cloud Spanner.
   */
  public SpannerRow(Struct struct) {
    this.struct = struct;
  }

  @VisibleForTesting
  Struct getStruct() {
    return this.struct;
  }

  @Override
  public <T> T get(Object identifier, Class<T> type) {
    return null;
  }
}

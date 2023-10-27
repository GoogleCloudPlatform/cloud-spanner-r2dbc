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
import static io.r2dbc.spi.TransactionDefinition.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.spanner.TimestampBound;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;



class SpannerTransactionDefinitionTest {

  @Test
  void shouldThrowExceptionIfTimeStampBoundIsConfiguredWithReadWriteTransaction() {
    SpannerTransactionDefinition.Builder builder1 = new SpannerTransactionDefinition.Builder()
        .with(TIMESTAMP_BOUND, TimestampBound.ofExactStaleness(5, TimeUnit.SECONDS))
        .with(READ_ONLY, false);

    // absence of READ_ONLY attribute indicates read write transaction
    SpannerTransactionDefinition.Builder builder2 = new SpannerTransactionDefinition.Builder()
        .with(TIMESTAMP_BOUND, TimestampBound.ofExactStaleness(5, TimeUnit.SECONDS));

    assertThrows(IllegalArgumentException.class, builder1::build);
    assertThrows(IllegalArgumentException.class, builder2::build);
  }
}
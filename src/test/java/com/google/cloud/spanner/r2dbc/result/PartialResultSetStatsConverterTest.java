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

package com.google.cloud.spanner.r2dbc.result;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.ResultSetStats;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests the stats converter.
 */
public class PartialResultSetStatsConverterTest {

  final Value a1 = Value.newBuilder().setBoolValue(false).build();
  final Value a2 = Value.newBuilder().setStringValue("abc").build();

  ResultSetMetadata resultSetMetadata = ResultSetMetadata.newBuilder().setRowType(
      StructType.newBuilder()
          .addFields(Field.newBuilder().setName("boolField").build())
          .addFields(Field.newBuilder().setName("stringField").build())
          .build()
  ).build();

  @Test
  public void readOneResultSetQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        this.resultSetMetadata
    ).setChunkedValue(false)
        .addValues(this.a1)
        .addValues(this.a2).build();

    Flux<PartialResultSet> inputs = Flux.just(p1);

    assertThat(Mono.<Integer>create(
        sink -> inputs.subscribe(new PartialResultSetStatsConverter(sink)))
        .block()).isZero();
  }

  @Test
  public void readMultiResultSetQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        this.resultSetMetadata
    ).setChunkedValue(false)
        .addValues(this.a1).build();

    PartialResultSet p2 = PartialResultSet.newBuilder().setChunkedValue(false)
        .addValues(this.a2).build();

    Flux<PartialResultSet> inputs = Flux.just(p1, p2);

    assertThat(Mono.<Integer>create(
        sink -> inputs.subscribe(new PartialResultSetStatsConverter(sink)))
        .block()).isZero();
  }

  @Test
  public void readDmlQueryTest() {
    PartialResultSet p1 = PartialResultSet.newBuilder().setStats(
        ResultSetStats.newBuilder().setRowCountExact(555).build()
    ).build();

    Flux<PartialResultSet> inputs = Flux.just(p1);

    assertThat(Mono.<Integer>create(
        sink -> inputs.subscribe(new PartialResultSetStatsConverter(sink)))
        .block()).isEqualTo(555);
  }

}

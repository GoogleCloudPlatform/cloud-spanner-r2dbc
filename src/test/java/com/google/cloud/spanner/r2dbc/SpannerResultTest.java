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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Test for {@link SpannerResult}.
 */
public class SpannerResultTest {

  private Flux<Struct> resultSet;

  /**
   * Setup.
   */
  @Before
  public void setup() {
    Struct struct1 = Struct.newBuilder().set("id").to(Value.string("key1")).build();
    Struct struct2 = Struct.newBuilder().set("id").to(Value.string("key2")).build();
    this.resultSet = Flux.just(struct1, struct2);
  }

  @Test
  public void getRowsUpdatedTest() {
    assertThat(((Mono) new SpannerResult(this.resultSet).getRowsUpdated()).block()).isEqualTo(0);
    assertThat(((Mono) new SpannerResult(2).getRowsUpdated()).block()).isEqualTo(2);
  }

  @Test
  public void nullResultSetTest() {
    assertThatThrownBy(() -> new SpannerResult(null))
        .hasMessage("A non-null flux of rows is required.");
  }

  @Test
  public void mapTest() {
    assertThat(new SpannerResult(this.resultSet).map((row, metadata) ->
        ((SpannerRow) row).getStruct().getString("id") + "-" + ((SpannerRowMetadata) metadata)
            .getStruct().getString("id")).collectList().block())
        .containsExactly("key1-key1", "key2-key2");
  }

  @Test
  public void noResultsMapTest() {
    assertThat(new SpannerResult(2).map((x, y) -> "unused")).isEqualTo(Flux.empty());
  }

  static class MockResults {

    List<Struct> structs;

    int counter = -1;

    boolean next() {
      if (this.counter < this.structs.size() - 1) {
        this.counter++;
        return true;
      }
      return false;
    }

    Struct getCurrent() {
      return this.structs.get(this.counter);
    }
  }
}

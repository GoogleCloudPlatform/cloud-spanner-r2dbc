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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SpannerResult}.
 *
 * @author Chengyuan Zhao
 */
public class SpannerResultTest {

  private ResultSet resultSet;

  @Before
  public void setup() {
    Struct struct1 = Struct.newBuilder().set("id").to(Value.string("key1")).build();
    Struct struct2 = Struct.newBuilder().set("id").to(Value.string("key2")).build();

    MockResults mockResults = new MockResults();
    mockResults.structs = Arrays.asList(struct1, struct2);
    this.resultSet = mock(ResultSet.class);
    when(this.resultSet.next()).thenAnswer((invocation) -> mockResults.next());
    when(this.resultSet.getCurrentRowAsStruct())
        .thenAnswer((invocation) -> mockResults.getCurrent());
  }

  @Test
  public void getRowsUpdatedTest() {
    assertThat(((Mono)new SpannerResult(this.resultSet).getRowsUpdated()).block()).isEqualTo(0);
    assertThat(((Mono)new SpannerResult(2).getRowsUpdated()).block()).isEqualTo(2);
  }

  @Test
  public void mapDummyImplementation() {
    SpannerResult result = new SpannerResult();
    Flux<String> mappingResult = result.map((row, rowMetadata) -> "dummy result");
    assertThat(mappingResult).isNotNull();
    assertThat(mappingResult.blockFirst()).isEqualTo("dummy result");
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

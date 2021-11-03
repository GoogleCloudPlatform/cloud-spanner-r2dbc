/*
 * Copyright 2020-2021 Google LLC
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

import com.google.cloud.spanner.Value;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;

class JsonHolderTest {

  static Stream<JsonHolder> values() {
    return Stream.of(JsonHolder.of("json-string"), new JsonHolder("json-string"));
  }

  @MethodSource("values")
  @ParameterizedTest
  void testConsumeToString(JsonHolder json) {
    assertThat(json.toString()).isEqualTo("json-string");
  }

  @MethodSource("values")
  @ParameterizedTest
  void testConsume2(JsonHolder json) {
    assertThat(json.getJsonVal()).isInstanceOf(Value.class);
  }
}

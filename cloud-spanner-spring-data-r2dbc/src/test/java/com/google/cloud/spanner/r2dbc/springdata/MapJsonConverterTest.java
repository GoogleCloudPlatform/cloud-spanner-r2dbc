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

package com.google.cloud.spanner.r2dbc.springdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.spanner.r2dbc.v2.JsonWrapper;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Test for converters. */
class MapJsonConverterTest {
  private Gson gson = new Gson();

  @Test
  void jsonToMapConverterTest() {
    JsonToMapConverter converter = new JsonToMapConverter(this.gson);
    Map<Object, Object> resultMap =
        converter.convert(
            JsonWrapper.of("{\"a\":\"a string\",\"b\":9, \"c\" : 12.537, \"d\" : true}"));
    assertThat(resultMap)
        .isInstanceOf(Map.class)
        .hasSize(4)
        .containsEntry("a", "a string")
        .containsEntry("b", 9.0)
        .containsEntry("c", 12.537)
        .containsEntry("d", true);

    // Convert should fail: duplicate keys not allowed
    assertThatThrownBy(
            () -> converter.convert(JsonWrapper.of("{\"a\":\"a string\","
                    + "\"a\":\"another string\"}")))
        .isInstanceOf(JsonSyntaxException.class);
  }

  @Test
  void mapToJsonConverterTest() {
    MapToJsonConverter converter = new MapToJsonConverter(this.gson);
    Map<Object, Object> mapToConvert =
        ImmutableMap.of("a", "a string", "b", 9, "c", 12.537, "d", true);
    assertThat(converter.convert(mapToConvert))
        .isEqualTo(JsonWrapper.of("{\"a\":\"a string\",\"b\":9,\"c\":12.537,\"d\":true}"));
  }
}

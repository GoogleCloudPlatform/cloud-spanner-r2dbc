/*
 * Copyright 2021 Google LLC
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

package com.example;

import com.google.cloud.spanner.r2dbc.v2.JsonWrapper;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ReadingConverter
public class JsonToMapConverter implements Converter<JsonWrapper, Map<String, Object>> {

  private final Gson gson;

  @Autowired
  public JsonToMapConverter(Gson gson) {
    this.gson = gson;
  }

  @Override
  public Map<String, Object> convert(JsonWrapper json) {
    try {
      return gson.fromJson(json.toString(), Map.class);
    } catch (JsonParseException e) {
      return new HashMap<>();
    }
  }
}

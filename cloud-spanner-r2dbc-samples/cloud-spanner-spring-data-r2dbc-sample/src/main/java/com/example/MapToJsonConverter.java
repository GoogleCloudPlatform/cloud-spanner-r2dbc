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

import com.google.cloud.spanner.r2dbc.v2.JsonHolder;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@WritingConverter
public class MapToJsonConverter implements Converter<Map<String, Object>, JsonHolder> {

  private final Gson gson;

  @Autowired
  public MapToJsonConverter(Gson gson) {
    this.gson = gson;
  }

  @Override
  public JsonHolder convert(Map<String, Object> source) {
    try {
      return JsonHolder.of(gson.toJson(source));
    } catch (JsonParseException e) {
      return JsonHolder.of("");
    }
  }
}

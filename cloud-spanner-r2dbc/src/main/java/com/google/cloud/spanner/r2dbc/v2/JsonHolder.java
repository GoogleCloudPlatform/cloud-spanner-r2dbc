/*
 * Copyright 2021-2021 Google LLC
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

/** Wrapper class to hold Json value. */
public class JsonHolder {
  //  private Value jsonVal;
  private String jsonString;

  public JsonHolder(String jsonString) {
    //    this.jsonVal = Value.json(jsonString);
    this.jsonString = jsonString;
  }

  public Value getJsonVal() {
    //    return this.jsonVal;
    return Value.json(this.jsonString);
  }

  public String getJsonString() {
    return this.jsonString;
  }

  public static JsonHolder of(String jsonString) {
    return new JsonHolder(jsonString);
  }

  @Override
  public String toString() {
    //    return this.jsonVal.getJson();
    return this.jsonString;
  }
}

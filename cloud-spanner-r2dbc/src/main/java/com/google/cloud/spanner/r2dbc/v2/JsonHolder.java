package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.Value;

public class JsonHolder {
  private Value jsonVal;

  public JsonHolder(String jsonString) {
    this.jsonVal = Value.json(jsonString);
  }

  public Value getJsonVal() {
    return jsonVal;
  }

  public static JsonHolder of(String jsonString) {
    return new JsonHolder(jsonString);
  }

  @Override
  public String toString() {
    return this.jsonVal.getJson();
  }
}

package com.google.cloud.spanner.r2dbc.v2;

import com.google.cloud.spanner.Value;

import java.nio.ByteBuffer;

public class Json {
    private Value jsonVal;

    public Json(Value jsonVal) {
        this.jsonVal = jsonVal;
    }
    public Json(String jsonString) {
        this.jsonVal = Value.json(jsonString);
    }

    public void setJsonVal(Value jsonVal) {
        this.jsonVal = jsonVal;
    }

    public Value getJsonVal() {
        return jsonVal;
    }

    public static Json of(String jsonString) {
        return new Json(Value.json(jsonString));
    }

    @Override
    public String toString() {
//        return "Json {}";
        return this.jsonVal.getJson();
    }
}

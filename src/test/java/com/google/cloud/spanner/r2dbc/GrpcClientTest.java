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

import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import reactor.core.publisher.Flux;

/**
 * Tests the GRPC client class.
 */
public class GrpcClientTest {

  @Test
  public void assembleRowsTest(){

    // These are the expected final rows' values
    Value a1 = Value.newBuilder().setBoolValue(false).build();
    Value a2 = Value.newBuilder().setStringValue("abc").build();
    Value a3 = Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
        Value.newBuilder().setStringValue("12").build(),
        Value.newBuilder().setStringValue("34").build(),
        Value.newBuilder().setStringValue("56").build())).build()).build();

    Value b1 = Value.newBuilder().setBoolValue(true).build();
    Value b2 = Value.newBuilder().setStringValue("xyz").build();
    Value b3 = Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
        Value.newBuilder().setStringValue("78").build(),
        Value.newBuilder().setStringValue("910").build(),
        Value.newBuilder().setStringValue("1122").build())).build()).build();

    // The values above will be split across several partial result sets.
    PartialResultSet p1 = PartialResultSet.newBuilder().setMetadata(
        ResultSetMetadata.newBuilder().setRowType(
            StructType.newBuilder()
                .addFields(Field.newBuilder().build())
                .addFields(Field.newBuilder().build())
                .addFields(Field.newBuilder().build())
                .build()
        ).build()
    ).setChunkedValue(false)
        .addValues(a1).build();

    PartialResultSet p2 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("a")).setChunkedValue(true).build();

    PartialResultSet p3 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("b")).setChunkedValue(true).build();

    PartialResultSet p4 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("c"))
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(Arrays.asList(
            Value.newBuilder().setStringValue("12").build(),
            Value.newBuilder().setStringValue("34").build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p5 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setStringValue("56").build())).build()).build())
        .addValues(Value.newBuilder().setBoolValue(true))
        .addValues(Value.newBuilder().setStringValue("xy"))
        .setChunkedValue(true).build();

    PartialResultSet p6 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("z"))
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setStringValue("78").build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p7 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setStringValue("910").build())).build()).build())
        .setChunkedValue(true).build();

    PartialResultSet p8 = PartialResultSet.newBuilder()
        .addValues(Value.newBuilder().setStringValue("1122"))
        .addValues(Value.newBuilder().setListValue(ListValue.newBuilder().addAllValues(
            Collections.singletonList(
                Value.newBuilder().setStringValue("78").build())).build()).build())
        .setChunkedValue(false).build();

    Flux<PartialResultSet> results = Flux.fromArray(new PartialResultSet[]{
        p1,p2,p3,p4,p5,p6,p7,p8
    });

  }

}
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

package com.google.cloud.spanner.r2dbc.codecs;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DefaultCodecsTest {

  private Codecs codecs = new DefaultCodecs();

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
				{ new boolean[] { true, false, true }, boolean[].class, Type.array(Type.bool()) },
				{ new ByteArray[] { ByteArray.copyFrom("ab"), ByteArray.copyFrom("cd") }, ByteArray[].class,
						Type.array(Type.bytes()) },
				{ new Date[] { Date.fromYearMonthDay(800, 12, 31), Date.fromYearMonthDay(2019, 1, 1) }, Date[].class,
						Type.array(Type.date()) },
				{ new double[] { 2.0d, 3.0d }, double[].class, Type.array(Type.float64()) },
				{ new long[] { 2L, 1003L }, long[].class, Type.array(Type.int64()) },
				{ new java.sql.Timestamp[] { java.sql.Timestamp.valueOf("2013-08-04 12:00:01"),
						java.sql.Timestamp.valueOf("1800-02-01 14:02:31") }, java.sql.Timestamp[].class,
						Type.array(Type.timestamp()) },
				{ new String[] { "abc", "def" }, String[].class, Type.array(Type.string()) },
				{ new Timestamp[] { Timestamp.ofTimeMicroseconds(1234), Timestamp.ofTimeMicroseconds(123456) },
						Timestamp[].class, Type.array(Type.timestamp()) },
				{ true, Boolean.class, Type.bool() },
				{ Boolean.FALSE, Boolean.class, Type.bool() },
				{ ByteArray.copyFrom("ab"), ByteArray.class, Type.bytes() },
				{ Date.fromYearMonthDay(800, 12, 31), Date.class, Type.date() },
				{ 2.0d, Double.class, Type.float64() },
				{ 12345L, Long.class, Type.int64() },
				{ java.sql.Timestamp.valueOf("2013-08-04 12:00:01"), java.sql.Timestamp.class, Type.timestamp() },
				{ "abc", String.class, Type.string() },
				{ Timestamp.ofTimeMicroseconds(123456), Timestamp.class, Type.timestamp() }
    });
  }

  @Parameter
  public Object val;

  @Parameter(1)
  public Class<?> type;

  @Parameter(2)
  public Type valueType;


  @Test
  public void codecsTest() {
    Value value = codecs.encode(val);
    Value nullValue = codecs.encodeNull(valueType);
    Struct row = Struct.newBuilder()
        .set("field").to(value)
        .set("nullField").to(nullValue)
        .build();


    assertThat(value.getType()).isEqualTo(valueType);
    assertThat(codecs.decode(row, "field", type)).isEqualTo(val);
    assertThat(codecs.decode(row, 0, type)).isEqualTo(val);

    assertThat(nullValue.getType()).isEqualTo(valueType);
    assertThat(codecs.decode(row, "nullField", type)).isNull();
    assertThat(codecs.decode(row, 1, type)).isNull();
  }

}

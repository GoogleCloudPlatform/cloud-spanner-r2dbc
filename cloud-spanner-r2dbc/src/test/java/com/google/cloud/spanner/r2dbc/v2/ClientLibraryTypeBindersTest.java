/*
 * Copyright 2019-2020 Google LLC
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement.Builder;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.ValueBinder;
import com.google.cloud.spanner.r2dbc.BindingFailureException;
import com.google.cloud.spanner.r2dbc.statement.TypedNull;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import java.math.BigDecimal;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ClientLibraryTypeBindersTest {
  ValueBinder valueBinder;

  ValueBinder nullBinder;

  Builder statementBuilder;

  @BeforeEach
  public void setUp() {
    this.valueBinder = Mockito.mock(ValueBinder.class);
    this.nullBinder = Mockito.mock(ValueBinder.class);

    this.statementBuilder = Mockito.mock(Builder.class);
    when(this.statementBuilder.bind("valueColumn")).thenReturn(this.valueBinder);
    when(this.statementBuilder.bind("nullColumn")).thenReturn(this.nullBinder);
  }

  @Test
  void unsupportedTypeThrowsException() {
    Random rand = new Random();
    assertThatThrownBy(() -> ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", rand))
        .isInstanceOf(BindingFailureException.class)
        .hasMessageContaining("Can't find a binder for type: class java.util.Random");

    TypedNull randNull = new TypedNull(Random.class);
    assertThatThrownBy(
        () -> ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", randNull))
          .isInstanceOf(BindingFailureException.class)
          .hasMessageContaining("Can't find a binder for type: class java.util.Random");
  }

  @Test
  void longBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", 1L);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Long.class));
    verify(this.valueBinder).to((Long) 1L);
    verify(this.nullBinder).to((Long) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void doubleBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", 2.0);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Double.class));
    verify(this.valueBinder).to((Double) 2.0);
    verify(this.nullBinder).to((Double) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void booleanBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", true);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Boolean.class));
    verify(this.valueBinder).to((Boolean) true);
    verify(this.nullBinder).to((Boolean) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void byteArrayBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", ByteArray.copyFrom("abc"));
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(ByteArray.class));
    verify(this.valueBinder).to(ByteArray.copyFrom("abc"));
    verify(this.nullBinder).to((ByteArray) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void dateBinderTest() {
    Date date = Date.fromYearMonthDay(1992, 12, 31);
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", date);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Date.class));
    verify(this.valueBinder).to(date);
    verify(this.nullBinder).to((Date) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void stringBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", "abc");
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(String.class));
    verify(this.valueBinder).to("abc");
    verify(this.nullBinder).to((String) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void timestampBinderTest() {
    Timestamp ts = Timestamp.ofTimeMicroseconds(123456);
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", ts);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Timestamp.class));
    verify(this.valueBinder).to(ts);
    verify(this.nullBinder).to((Timestamp) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void bigDecimalBinderTest() {
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", BigDecimal.TEN);
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(BigDecimal.class));
    verify(this.valueBinder).to(BigDecimal.TEN);
    verify(this.nullBinder).to((BigDecimal) null);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void integerBindsAsLong() {

    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", 123);
    verify(this.valueBinder).to((Long) 123L);
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void integerNullBindsAsLong() {

    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(Integer.class));
    verify(this.nullBinder).to((Long) null);
    verifyNoMoreInteractions(this.valueBinder);

  }

  @Test
  void jsonBinderTest() {
    ClientLibraryBinder.bind(
        this.statementBuilder, "valueColumn", JsonWrapper.of("{\"rating\":9,\"open\":true}"));
    ClientLibraryBinder.bind(this.statementBuilder, "nullColumn", new TypedNull(JsonWrapper.class));
    verify(this.valueBinder).to(Value.json("{\"rating\":9,\"open\":true}"));
    verify(this.nullBinder).to(Value.json(null));
    verifyNoMoreInteractions(this.valueBinder);
  }

  @Test
  void stringArrayBinderTest() {
    String[] value = new String[] { "Apples", "Oranges" };

    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn", value);
    ClientLibraryBinder.bind(
        this.statementBuilder, "nullColumn", new TypedNull(String[].class));

    ArgumentCaptor<Iterable<String>> captor = ArgumentCaptor.forClass(Iterable.class);
    verify(this.valueBinder).toStringArray(captor.capture());
    assertThat(captor.getValue()).containsExactly("Apples", "Oranges");

    verify(this.nullBinder).toStringArray(null);
  }

  @Test
  void bindingWithParameter() {
    String text = "something there is that doesn't love a wall";
    ClientLibraryBinder.bind(this.statementBuilder, "valueColumn",
        Parameters.in(R2dbcType.VARCHAR, text));
    verify(this.valueBinder).to(text);
  }
}

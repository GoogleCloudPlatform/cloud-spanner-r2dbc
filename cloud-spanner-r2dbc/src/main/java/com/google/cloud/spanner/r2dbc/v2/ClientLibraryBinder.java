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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.r2dbc.BindingFailureException;
import com.google.cloud.spanner.r2dbc.SpannerType;
import com.google.cloud.spanner.r2dbc.statement.TypedNull;
import com.google.cloud.spanner.r2dbc.util.Assert;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class ClientLibraryBinder {
  private static final List<ClientLibraryTypeBinder> binders = buildBinders();

  private static List<ClientLibraryTypeBinder> buildBinders() {
    List<ClientLibraryTypeBinder> binders = new ArrayList<>();
    binders.add(
        new ClientLibraryTypeBinderImpl<>(Integer.class,
            (binder, val) -> binder.to(longFromInteger(val))));
    binders.add(new ClientLibraryTypeBinderImpl<>(Long.class, (binder, val) -> binder.to(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(Double.class, (binder, val) -> binder.to(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(Boolean.class, (binder, val) -> binder.to(val)));
    binders.add(
        new ClientLibraryTypeBinderImpl<>(ByteArray.class, (binder, val) -> binder.to(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(Date.class, (binder, val) -> binder.to(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(String.class, (binder, val) -> binder.to(val)));
    binders.add(
        new ClientLibraryTypeBinderImpl<>(Timestamp.class, (binder, val) -> binder.to(val)));
    binders.add(
        new ClientLibraryTypeBinderImpl<>(BigDecimal.class, (binder, val) -> binder.to(val)));

    binders.add(
        new ClientLibraryTypeBinderImpl<>(
            JsonWrapper.class,
            (binder, val) -> binder.to(val == null ? Value.json(null) : val.getJsonVal())));

    // Primitive arrays
    binders.add(new ClientLibraryTypeBinderImpl<>(
        boolean[].class, (binder, val) -> binder.toBoolArray(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(
        long[].class, (binder, val) -> binder.toInt64Array(val)));
    binders.add(new ClientLibraryTypeBinderImpl<>(
        double[].class, (binder, val) -> binder.toFloat64Array(val)));

    // Primitive arrays that have to expand element size to 64 bits to match Spanner types.
    binders.add(new ClientLibraryTypeBinderImpl<>(
        int[].class, (binder, val) -> binder.toInt64Array(Arrays.asList(val))));
    binders.add(new ClientLibraryTypeBinderImpl<>(
        float[].class, (binder, val) -> binder.toFloat64Array(Arrays.asList(val))));

    // Object arrays
    binders.add(new ArrayToIterableBinder<>(Boolean[].class,
        (binder, iterable) -> binder.toBoolArray(iterable)));
    binders.add(new ArrayToIterableBinder<>(ByteArray[].class,
        (binder, iterable) -> binder.toBytesArray(iterable)));
    binders.add(new ArrayToIterableBinder<>(
        Date[].class, (binder, iterable) -> binder.toDateArray(iterable)));
    binders.add(new ArrayToIterableBinder<>(String[].class,
        (binder, iterable) -> binder.toStringArray(iterable)));
    binders.add(new ArrayToIterableBinder<>(
        Timestamp[].class, (binder, iterable) -> binder.toTimestampArray(iterable)));
    binders.add(new ArrayToIterableBinder<>(
        BigDecimal[].class, (binder, iterable) -> binder.toNumericArray(iterable)));

    // STRUCT not supported

    // Binds collections to Spanner array, if an element type hint is available.
    binders.add(new IterableBinder());

    return binders;
  }

  static void bind(Statement.Builder builder, String name, Object value) {
    Assert.requireNonNull(name, "Column name must not be null");
    Assert.requireNonNull(value, "Value must not be null");

    Object finalValue;
    Class<?> valueClass;

    final SpannerType spannerType;

    if (value instanceof Parameter) {
      Parameter param = (Parameter) value;
      finalValue = param.getValue();
      Type type = param.getType();
      spannerType = type instanceof SpannerType ? (SpannerType) param.getType() : null;
      valueClass = type.getJavaType();
    } else if (isTypedNull(value)) {
      finalValue = null;
      valueClass = ((TypedNull) value).getType();
      spannerType = null;
    } else {
      finalValue = value;
      valueClass = value.getClass();
      spannerType = null;
    }

    Optional<ClientLibraryTypeBinder> optionalBinder = binders.stream()
        .filter(e -> e.canBind(valueClass, spannerType))
        .findFirst();
    if (!optionalBinder.isPresent()) {
      throw new BindingFailureException("Can't find a binder for type: " + valueClass);
    }

    optionalBinder.get().bind(builder, name, finalValue, spannerType);
  }

  private static boolean isTypedNull(Object value) {
    return value.getClass().equals(TypedNull.class);
  }

  private static Long longFromInteger(Integer intValue) {
    return intValue == null ? null : intValue.longValue();
  }
}

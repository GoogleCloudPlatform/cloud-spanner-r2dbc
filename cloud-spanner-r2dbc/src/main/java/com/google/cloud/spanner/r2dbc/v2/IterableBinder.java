/*
 * Copyright 2022-2022 Google LLC
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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.r2dbc.BindingFailureException;
import com.google.cloud.spanner.r2dbc.SpannerType;

/**
 * Binds {@link Iterable} values to statement parameters, using Spanner type information as a hint.
 *
 * @param <T> a typed {@link Iterable}
 */
class IterableBinder<T extends Iterable<?>> implements ClientLibraryTypeBinder<T> {

  @Override
  public boolean canBind(Class<T> type, SpannerType spannerType) {
    // Handling all iterables regardless of SpannerType will allow more useful error messages.
    return Iterable.class.isAssignableFrom(type);
  }

  @Override
  public void bind(Statement.Builder builder, String name, T value, SpannerType spannerType) {
    if (spannerType == null) {
      throw new BindingFailureException(
          "When binding collections, Parameters.in(SpannerType.of(Type.array(...))) must be used.");
    }
    spannerType.bindIterable(builder.bind(name), value);
  }
}

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

package com.google.cloud.spanner.r2dbc.springdata;

import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.r2dbc.v2.JsonWrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.lang.NonNull;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/**
 * The {@link R2dbcDialect} implementation which enables usage of Spring Data R2DBC with Cloud
 * Spanner.
 */
public class SpannerR2dbcDialect extends AbstractDialect implements R2dbcDialect {
  static final BindMarkersFactory NAMED =
      BindMarkersFactory.named("@", "val", 32);

  public static final String SQL_LIMIT = "LIMIT ";

  private static final LimitClause LIMIT_CLAUSE = new LimitClause() {
    @Override
    public String getLimit(long limit) {
      return SQL_LIMIT + limit;
    }

    @Override
    public String getOffset(long offset) {
      return SQL_LIMIT + Long.MAX_VALUE + " OFFSET " + offset;
    }

    @Override
    public String getLimitOffset(long limit, long offset) {
      return SQL_LIMIT + limit + " OFFSET " + offset;
    }

    @Override
    public Position getClausePosition() {
      return Position.AFTER_ORDER_BY;
    }
  };

  /**
   * Pessimistic locking is not supported.
   * Spanner has a LOCK_SCANNED_RANGES hint, but it appears before SELECT, a position not currently
   * supported in LockClause.Position
   */
  private static final LockClause LOCK_CLAUSE = new LockClause() {
    @Override
    public String getLock(LockOptions lockOptions) {
      return "";
    }

    @Override
    public Position getClausePosition() {
      // It does not matter where to append an empty string.
      return Position.AFTER_FROM_TABLE;
    }
  };

  @Override
  public BindMarkersFactory getBindMarkersFactory() {
    return NAMED;
  }

  @Override
  public LimitClause limit() {
    return LIMIT_CLAUSE;
  }

  @Override
  public LockClause lock() {
    return LOCK_CLAUSE;
  }

  @WritingConverter
  private enum JsonToValueConverter implements Converter<JsonWrapper, Value> {

    INSTANCE;

    @Override
    @NonNull
    public Value convert(JsonWrapper source) {
      return Value.json(source.toString());
    }
  }

  @ReadingConverter
  private enum StringToJsonConverter implements Converter<String, JsonWrapper> {

    INSTANCE;

    @Override
    @NonNull
    public JsonWrapper convert(String source) {
      return JsonWrapper.of(source);
    }
  }

  @Override
  public Collection<Object> getConverters() {
    //    Collection<Object> converters = R2dbcDialect.super.getConverters();
    //    converters.add(JsonToStringConverter.INSTANCE);
    List<Object> converters = new ArrayList<>();
    //    converters.add(JsonToValueConverter.INSTANCE);
    //    converters.add(StringToJsonConverter.INSTANCE);
    return converters;
  }

  @Override
  public Collection<? extends Class<?>> getSimpleTypes() {

    return Arrays.asList(JsonWrapper.class);
    //    return Arrays.asList(Value.class);
  }
}

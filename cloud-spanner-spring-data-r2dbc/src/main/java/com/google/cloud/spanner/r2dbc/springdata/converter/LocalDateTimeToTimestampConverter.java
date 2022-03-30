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

package com.google.cloud.spanner.r2dbc.springdata.converter;

import com.google.cloud.Timestamp;
import java.time.LocalDateTime;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

/** {@link LocalDateTime} to {@link Timestamp} writing converter. */
@WritingConverter
public class LocalDateTimeToTimestampConverter implements Converter<LocalDateTime, Timestamp> {

  @Override
  public Timestamp convert(LocalDateTime source) {
    return Timestamp.parseTimestamp(source.toString());
  }
}

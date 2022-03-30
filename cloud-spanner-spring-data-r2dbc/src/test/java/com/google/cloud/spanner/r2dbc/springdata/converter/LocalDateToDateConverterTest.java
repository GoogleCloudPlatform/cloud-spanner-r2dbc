/*
 * Copyright 2020-2021 Google LLC
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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.Date;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class LocalDateToDateConverterTest {

  private final LocalDateToDateConverter converter = new LocalDateToDateConverter();

  @Test
  void shouldConvertSuccessfully() {
    Date expected = Date.parseDate("2022-03-30");
    Date actual = this.converter.convert(LocalDate.parse("2022-03-30"));
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void shouldFailToConvert() {
    Date expected = Date.parseDate("2022-03-30");
    Date actual = this.converter.convert(LocalDate.parse("2022-03-31"));
    assertThat(actual).isNotEqualTo(expected);
  }
}
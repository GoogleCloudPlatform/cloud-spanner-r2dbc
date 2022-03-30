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

import com.google.cloud.Timestamp;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LocalDateTimeToTimestampTest {

  private final LocalDateTimeToTimestampConverter converter =
      new LocalDateTimeToTimestampConverter();

  @ParameterizedTest(name = "expectedValue={0}, localDateTimeValue={1}")
  @MethodSource("positiveTestArgs")
  void shouldConvertSuccessfully(String expectedValue, String localDateTimeValue) {
    Timestamp expected = Timestamp.parseTimestamp(expectedValue);
    Timestamp actual = this.converter.convert(LocalDateTime.parse(localDateTimeValue));
    assertThat(actual).isEqualTo(expected);
  }

  @ParameterizedTest(name = "expectedValue={0}, localDateTimeValue={1}")
  @MethodSource("negativeTestArgs")
  void shouldFailToConvert(String expectedValue, String localDateTimeValue) {
    Timestamp expected = Timestamp.parseTimestamp(expectedValue);
    Timestamp actual = this.converter.convert(LocalDateTime.parse(localDateTimeValue));
    assertThat(actual).isNotEqualTo(expected);
  }

  private static Stream<Arguments> positiveTestArgs() {
    return Stream.of(
      Arguments.of("2022-03-30T12:00:30.848Z", "2022-03-30T12:00:30.848"),
      Arguments.of("2022-03-30T12:00:30Z", "2022-03-30T12:00:30")
    );
  }

  private static Stream<Arguments> negativeTestArgs() {
    return Stream.of(
      Arguments.of("2022-03-30T10:00:30.848Z", "2022-03-30T12:00:30.848")
    );
  }
}
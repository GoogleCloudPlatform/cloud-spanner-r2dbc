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

package com.google.cloud.spanner.r2dbc.springdata.it.entities;

import java.util.Map;
import org.springframework.data.relational.core.mapping.Column;

/**
 * Example entity.
 */
public class Person {

  @Column("NAME")
  private String name;

  @Column("BIRTH_YEAR")
  private long birthYear;

  @Column("EXTRAS")
  private Map<String, String> extras;

  /**
   * Constructor.
   *
   * @param name name
   * @param birthYear birth year.
   * @param extras extra info stored in Map.
   */
  public Person(String name, long birthYear, Map<String, String> extras) {
    this.name = name;
    this.birthYear = birthYear;
    this.extras = extras;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getBirthYear() {
    return this.birthYear;
  }

  public void setBirthYear(long birthYear) {
    this.birthYear = birthYear;
  }

  public Map<String, String> getExtras() {
    return this.extras;
  }

  public void setExtras(Map<String, String> extras) {
    this.extras = extras;
  }

  @Override
  public String toString() {
    return "President{"
        + "name='"
        + this.name
        + '\''
        + ", birthYear="
        + this.birthYear
        + ", extras="
        + (this.getExtras() == null ? " " : this.getExtras().toString())
        + '}';
  }
}

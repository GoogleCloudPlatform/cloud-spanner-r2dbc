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

import org.springframework.data.relational.core.mapping.Column;

import java.util.Map;

/**
 * Example entity.
 */
public class Person {

  @Column("NAME")
  private String name;

  @Column("START_YEAR")
  private long startYear;

  @Column("EXTRAS")
//  private JsonWrapper extras;
  private Map<String, String> extras;

  public Person(String name, long startYear) {
    this.name = name;
    this.startYear = startYear;
  }

  public Person() {}

  public Person(String name, long startYear, Map<String, String> extras) {
    this.name = name;
    this.startYear = startYear;
    this.extras = extras;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getStartYear() {
    return this.startYear;
  }

  public void setStartYear(long startYear) {
    this.startYear = startYear;
  }

  public Map<String, String> getExtras() {
    return extras;
  }

  public void setExtras(Map<String, String> extras) {
    this.extras = extras;
  }

  @Override
  public String toString() {
    return "President{"
        + "name='"
        + this.name + '\''
        + ", startYear="
        + this.startYear
            + ", extras="
            + this.getExtras()==null? " " : this.getExtras().toString()
            + '}';
  }
}

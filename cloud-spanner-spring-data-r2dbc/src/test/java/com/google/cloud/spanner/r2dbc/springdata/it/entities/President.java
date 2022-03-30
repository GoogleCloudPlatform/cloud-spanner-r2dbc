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

import java.time.LocalDate;
import java.time.LocalDateTime;
import org.springframework.data.relational.core.mapping.Column;

/**
 * Example entity.
 */
public class President {

  @Column("NAME")
  private String name;

  @Column("START_YEAR")
  private int startYear;

  @Column("START_DATE")
  private LocalDate startDate;

  @Column("CREATED_AT")
  private LocalDateTime createdAt;

  /**
   * Constructor.
   *
   * @param name name
   * @param createdAt createdAt
   */
  public President(String name, LocalDateTime createdAt) {
    this.name = name;
    this.startYear = createdAt.getYear();
    this.startDate = createdAt.toLocalDate();
    this.createdAt = createdAt;
  }

  public String getName() {
    return this.name;
  }

  public int getStartYear() {
    return this.startYear;
  }

  public LocalDate getStartDate() {
    return this.startDate;
  }

  public LocalDateTime getCreatedAt() {
    return this.createdAt;
  }
}

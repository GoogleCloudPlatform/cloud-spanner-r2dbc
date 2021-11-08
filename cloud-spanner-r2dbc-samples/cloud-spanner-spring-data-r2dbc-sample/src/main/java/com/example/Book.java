/*
 * Copyright 2020 Google LLC
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

package com.example;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import java.util.Map;

/**
 * Book entity.
 */
@Table
public class Book {

  @Id
  @Column("ID")
  private String id;

  @Column("TITLE")
  private String title;

  @Column("EXTRADETAILS")
  private Map<String, String> extraDetails;

  public Book(String id, String title) {
    this.id = id;
    this.title = title;
  }

  public Book(String id, String title, Map<String, String> extraDetails) {
    this.id = id;
    this.title = title;
    this.extraDetails = extraDetails;
  }

  public Book() {
  }

  public String getId() {
    return id;
  }

  public String getTitle() {
    return this.title;
  }


  public Map<String, String> getExtraDetails() {
    return extraDetails;
  }

  @Override
  public String toString() {
    return "Book{" +
            "id='" + id + '\'' +
            ", title='" + title + '\'' +
            ", extraDetails=" +(extraDetails == null ? "" : extraDetails.toString()) +
            '}';
  }
}

/*
 * Copyright 2019 Google LLC
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example R2DBC application using the Cloud Spanner R2DBC.
 */
public class SampleApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(SampleApplication.class);

  private static final String INSTANCE = System.getProperty("spanner.instance");

  private static final String DATABASE = System.getProperty("spanner.database");

  private static final String PROJECT = System.getProperty("gcp.project");

  /**
   * Runs through a list of database operations.
   */
  public static void main(String[] args) {

    if (!(validateProperty("spanner.instance", INSTANCE)
        & validateProperty("spanner.database", DATABASE)
        & validateProperty("gcp.project", PROJECT))) {
      System.exit(1);
    }


    LOGGER.info(
        "Performing Cloud Spanner operations on:\n\tProject:{}\n\tInstance:{}\n\tDatabase:{}",
        INSTANCE, DATABASE, PROJECT);

    BookExampleApp bookExampleApp = new BookExampleApp(INSTANCE, DATABASE, PROJECT);

    bookExampleApp.dropTableIfPresent();
    bookExampleApp.createTable();
    bookExampleApp.saveBooks();
    bookExampleApp.retrieveBooks();
  }

  private static boolean validateProperty(String name, String value) {
    if (value == null) {
      LOGGER.error("Please provide {} property.", name);
      return false;
    }
    return true;
  }
}

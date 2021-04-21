package com.example;

import com.google.cloud.ServiceOptions;

class DbConstants {
  static final String TEST_INSTANCE
      = System.getProperty("spanner.instance", "reactivetest");
  static final String TEST_DATABASE
      = System.getProperty("spanner.database", "met");
  static final String TEST_PROJECT
      = System.getProperty("gcp.project", ServiceOptions.getDefaultProjectId());
}

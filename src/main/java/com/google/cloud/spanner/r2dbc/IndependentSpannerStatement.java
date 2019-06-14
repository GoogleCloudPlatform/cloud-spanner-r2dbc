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

package com.google.cloud.spanner.r2dbc;

import com.google.cloud.spanner.r2dbc.client.Client;
import com.google.cloud.spanner.r2dbc.util.Assert;
import com.google.spanner.v1.Session;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;

/**
 * A `SpannerStatement` that is single-use for read and write.
 */
public class IndependentSpannerStatement extends SpannerStatement {

  private final SpannerConnection spannerConnection;

  /**
   * Constructor.
   *
   * @param client cloud spanner client to use for performing the query operation
   * @param session current cloud spanner session
   * @param spannerConnection the connection used to execute this single-use statement
   * @param sql the query to execute
   * @param config config about the database and instance to use
   */
  public IndependentSpannerStatement(
      Client client,
      Session session,
      SpannerConnection spannerConnection,
      String sql,
      SpannerConnectionConfiguration config) {
    super(client, session, null, sql, config);
    this.spannerConnection = Assert
        .requireNonNull(spannerConnection, "A non-null SpannerConnection is required.");
  }

  @Override
  public Publisher<? extends Result> execute() {
    return this.spannerConnection.beginTransaction().thenMany(super.execute())
        .delayUntil(r -> this.spannerConnection.commitTransaction());
  }

}

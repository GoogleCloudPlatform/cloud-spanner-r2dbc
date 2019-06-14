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

import com.google.protobuf.ByteString;

/**
 * Contract for objects encapsulating Spanner state relevant to a specific connection.
 * <ul>
 *   <li>Session is created per-connection and cannot be changed.
 *   <li>Transaction is initially {@code null}, and can change as connection begins and ends
 *   transactions.
 *   <li>monotonically increasing update sequence describes the order of DML statements within a
 *   transaction.
 * </ul>
 */
public interface StatementExecutionContext {

  public String getSessionName();

  public long nextSeqNum();

  public ByteString getTransactionId();

  public boolean isReadWrite();

  public boolean isPartitionedDml();
}

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

import com.google.cloud.spanner.r2dbc.client.TransactionType;
import com.google.spanner.v1.Transaction;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * A class to hold transaction-related data.
 */
public class SpannerTransactionContext {

  private final AtomicLong seqNum = new AtomicLong(0);

  private final Transaction transaction;

  private final TransactionType transactionType;

  private SpannerTransactionContext(
      Transaction transaction, TransactionType transactionType) {

    this.transaction = transaction;
    this.transactionType = transactionType;
  }

  public Transaction getTransaction() {
    return this.transaction;
  }

  public long nextSeqNum() {
    return this.seqNum.getAndIncrement();
  }

  public boolean isReadWrite() {
    return this.transactionType.getTransactionOptions().hasReadWrite();
  }

  /**
   * Creates the SpannerTransactionContext.
   * @param transaction spanner transaction
   * @return spanner transaction context
   */
  public static @Nullable SpannerTransactionContext from(
      Transaction transaction, TransactionType transactionType) {

    if (transaction == null) {
      return null;
    }
    return new SpannerTransactionContext(transaction, transactionType);
  }
}

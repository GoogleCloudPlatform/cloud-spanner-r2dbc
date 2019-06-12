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

package com.google.cloud.spanner.r2dbc.client;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.spanner.v1.TransactionOptions;
import com.google.spanner.v1.TransactionOptions.PartitionedDml;
import com.google.spanner.v1.TransactionOptions.ReadOnly;
import com.google.spanner.v1.TransactionOptions.ReadWrite;
import java.util.Objects;

/**
 * The transaction type describing the scope and permissions of a Spanner database transaction.
 */
public class TransactionType {

  private static final TransactionType READ_WRITE_TRANSACTION =
      new TransactionType(
          TransactionOptions.newBuilder()
              .setReadWrite(ReadWrite.getDefaultInstance())
              .build());

  private static final TransactionType PARTITIONED_DML_TRANSACTION =
      new TransactionType(
          TransactionOptions.newBuilder()
              .setPartitionedDml(PartitionedDml.getDefaultInstance())
              .build());

  private final TransactionOptions transactionOptions;

  private TransactionType(TransactionOptions transactionOptions) {
    this.transactionOptions = transactionOptions;
  }

  public TransactionOptions getTransactionOptions() {
    return this.transactionOptions;
  }

  /**
   * Returns a read-write Spanner transaction type.
   */
  public static TransactionType readWriteTransaction() {
    return READ_WRITE_TRANSACTION;
  }

  /**
   * Returns a partitioned DML Spanner transaction type.
   */
  public static TransactionType partitionedDmlTransaction() {
    return PARTITIONED_DML_TRANSACTION;
  }

  /**
   * Returns a read-only {@link TransactionType} builder. Use the builder methods to specify the
   * settings of the read-only transaction.
   */
  public static ReadOnlyTransactionBuilder readOnlyTransactionBuilder() {
    return new ReadOnlyTransactionBuilder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TransactionType that = (TransactionType) o;
    return Objects.equals(this.transactionOptions, that.transactionOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.transactionOptions);
  }

  public static class ReadOnlyTransactionBuilder {

    private ReadOnly.Builder readOnlyBuilder;

    private ReadOnlyTransactionBuilder() {
      this.readOnlyBuilder = ReadOnly.newBuilder();
    }

    /**
     * Read at a timestamp where all previously committed transactions are visible.
     */
    public ReadOnlyTransactionBuilder setStrongRead(boolean useStrongRead) {
      this.readOnlyBuilder.setStrong(useStrongRead);
      return this;
    }

    /**
     * Executes all reads at a timestamp >= {@code minReadTimestamp}.
     */
    public ReadOnlyTransactionBuilder setMinReadTimestamp(Timestamp minReadTimestamp) {
      this.readOnlyBuilder.setMinReadTimestamp(minReadTimestamp);
      return this;
    }

    /**
     * Read data at a timestamp >= NOW - {@code maxStaleness} seconds.
     */
    public ReadOnlyTransactionBuilder setMaxStaleness(Duration maxStaleness) {
      this.readOnlyBuilder.setMaxStaleness(maxStaleness);
      return this;
    }

    /**
     * Executes all reads at the given timestamp.
     */
    public ReadOnlyTransactionBuilder setReadTimestamp(Timestamp readTimestamp) {
      this.readOnlyBuilder.setReadTimestamp(readTimestamp);
      return this;
    }

    /**
     * Guarantees that all writes that have committed more than the specified {@code exactStaleness}
     * duration of time are visible.
     */
    public ReadOnlyTransactionBuilder setExactStaleness(Duration exactStaleness) {
      this.readOnlyBuilder.setExactStaleness(exactStaleness);
      return this;
    }

    /**
     * Builds a read-only {@link TransactionType} based on the builder settings.
     */
    public TransactionType build() {
      TransactionOptions transactionOptions =
          TransactionOptions.newBuilder().setReadOnly(this.readOnlyBuilder).build();
      return new TransactionType(transactionOptions);
    }
  }
}

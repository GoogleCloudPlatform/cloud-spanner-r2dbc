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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.spanner.v1.TransactionOptions.PartitionedDml;
import com.google.spanner.v1.TransactionOptions.ReadOnly;
import com.google.spanner.v1.TransactionOptions.ReadWrite;
import org.junit.Test;

public class TransactionTypeTest {

  @Test
  public void testReadWriteTransactionType() {
    TransactionType transactionType = TransactionType.readWriteTransaction();
    assertThat(transactionType.getTransactionOptions().getReadWrite())
        .isEqualTo(ReadWrite.getDefaultInstance());
  }

  @Test
  public void testPartitionedDmlType() {
    TransactionType transactionType = TransactionType.partitionedDmlTransaction();
    assertThat(transactionType.getTransactionOptions().getPartitionedDml())
        .isEqualTo(PartitionedDml.getDefaultInstance());
  }

  @Test
  public void testReadOnlyTransaction() {
    TransactionType transactionType =
        TransactionType.readOnlyTransactionBuilder()
            .setExactStaleness(Duration.newBuilder().setSeconds(10).build())
            .setMaxStaleness(Duration.newBuilder().setSeconds(11).build())
            .setStrongRead(true)
            .setReadTimestamp(Timestamp.newBuilder().setSeconds(12).build())
            .setMinReadTimestamp(Timestamp.newBuilder().setSeconds(13).build())
            .build();

    ReadOnly readOnly = transactionType.getTransactionOptions().getReadOnly();
    assertThat(readOnly).isEqualTo(
        ReadOnly.newBuilder()
            .setExactStaleness(Duration.newBuilder().setSeconds(10))
            .setMaxStaleness(Duration.newBuilder().setSeconds(11))
            .setStrong(true)
            .setReadTimestamp(Timestamp.newBuilder().setSeconds(12))
            .setMinReadTimestamp(Timestamp.newBuilder().setSeconds(13))
            .build());
  }
}

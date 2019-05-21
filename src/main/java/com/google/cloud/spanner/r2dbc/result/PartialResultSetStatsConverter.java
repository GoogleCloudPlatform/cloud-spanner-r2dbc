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

package com.google.cloud.spanner.r2dbc.result;

import com.google.spanner.v1.PartialResultSet;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.MonoSink;

/**
 * Extracts stats from partial result sets from Cloud Spanner.
 */
public class PartialResultSetStatsConverter implements CoreSubscriber<PartialResultSet> {

  private final MonoSink<Integer> monoSink;
  private Subscription spannerSubscription;
  private int updateCount;

  /**
   * Constructor.
   *
   * @param monoSink the mono sink.
   */
  public PartialResultSetStatsConverter(
      MonoSink<Integer> monoSink) {
    this.monoSink = monoSink;
  }

  @Override
  public void onSubscribe(Subscription s) {
    spannerSubscription = s;
    // initial result
    spannerSubscription.request(1);
  }

  @Override
  public void onNext(PartialResultSet partialResultSet) {
    // We can complete on the first call of this function because
    // only DML queries return updated count and always on the first/only PR.
    if (partialResultSet.hasStats()) {
      this.updateCount = Math.toIntExact(partialResultSet.getStats().getRowCountExact());
    }
    onComplete();
  }

  @Override
  public void onError(Throwable t) {
    monoSink.error(t);
  }

  @Override
  public void onComplete() {
    monoSink.success(updateCount);
  }
}

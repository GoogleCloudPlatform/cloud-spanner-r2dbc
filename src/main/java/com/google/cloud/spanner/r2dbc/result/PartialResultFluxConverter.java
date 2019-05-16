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

import com.google.cloud.spanner.r2dbc.SpannerRow;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 */
public class PartialResultFluxConverter {

  private Flux<PartialResultSet> results;

  private PartialResultRowExtractor rowExtractor = new PartialResultRowExtractor();

  private Subscription spannerSubscription;

  public PartialResultFluxConverter(Flux<PartialResultSet> results) {
    this.results = results;
  }

  // TODO: backpressure support will go here.
  public Flux<SpannerRow> toRows() {

    return Flux.create(sink -> {

      results.subscribe(new CoreSubscriber<PartialResultSet>() {

        @Override
        public void onSubscribe(Subscription subscription) {
          PartialResultFluxConverter.this.spannerSubscription = subscription;
          // initial result
          PartialResultFluxConverter.this.spannerSubscription.request(1);
        }

        @Override
        public void onNext(PartialResultSet partialResultSet) {

          PartialResultFluxConverter.this.rowExtractor
              .extractCompleteRows(partialResultSet).forEach(sink::next);

          // no demand management yet; just request one at a time
          PartialResultFluxConverter.this.spannerSubscription.request(1);
        }

        @Override
        public void onError(Throwable throwable) {
          sink.error(throwable);
        }

        @Override
        public void onComplete() {
          sink.complete();
        }
      });


    });
  }


}

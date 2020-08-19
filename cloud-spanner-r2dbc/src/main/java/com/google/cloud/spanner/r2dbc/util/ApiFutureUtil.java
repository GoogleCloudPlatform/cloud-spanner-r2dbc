package com.google.cloud.spanner.r2dbc.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class ApiFutureUtil {

  public static <T> Mono<T> convertFutureToMono(Supplier<ApiFuture<T>> futureSupplier, ExecutorService executorService,
      BiConsumer<MonoSink, T> successCallback) {

    return Mono.create(sink -> {
      ApiFuture future = futureSupplier.get();
      sink.onCancel(() -> future.cancel(true));

      ApiFutures.addCallback(future,
          new ApiFutureCallback<T>() {
            @Override
            public void onFailure(Throwable t) {
              sink.error(t);
            }

            @Override
            public void onSuccess(T result) {
              successCallback.accept(sink, result);
            }
          }, executorService);
    });

  }

  public static <T> Mono<T> convertFutureToMono(Supplier<ApiFuture<T>> future, ExecutorService executorService) {
    return convertFutureToMono(future, executorService, (sink, result) -> { sink.success(result); });
  }

}

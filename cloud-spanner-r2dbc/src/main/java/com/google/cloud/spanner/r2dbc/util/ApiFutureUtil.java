package com.google.cloud.spanner.r2dbc.util;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import reactor.core.publisher.MonoSink;

public class ApiFutureUtil {

  public static <T> void convertFutureToMono(MonoSink sink, ApiFuture<T> future, ExecutorService executorService,
      Consumer<T> callback) {
    ApiFutures.addCallback(future,
        new ApiFutureCallback<T>() {
          @Override
          public void onFailure(Throwable t) {
            sink.error(t);
          }

          @Override
          public void onSuccess(T result) {
            callback.accept(result);
          }
        }, executorService);
  }

  public static <T> void convertFutureToMono(MonoSink sink, ApiFuture<T> future, ExecutorService executorService) {
    convertFutureToMono(sink, future, executorService, unused -> sink.success());
  }

}

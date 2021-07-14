package com.google.cloud.spanner.r2dbc.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class SpannerClientLibraryResultTest {

  @Test
  void nullRowsNotAllowed() {
    assertThrows(IllegalArgumentException.class,
        () -> new SpannerClientLibraryResult(null, 42));
  }

  @Test
  void getRowsUpdatedReturnsCorrectNumber() {
    SpannerClientLibraryResult result = new SpannerClientLibraryResult(Flux.empty(), 42);
    StepVerifier.create(result.getRowsUpdated())
        .expectNext(42)
    .verifyComplete();
  }

  @Test
  void mapGeneratesMetadataOnlyOnFirstCall() {
    SpannerClientLibraryRow mockRow1 = mock(SpannerClientLibraryRow.class);
    when(mockRow1.get("col")).thenReturn("value1");
    RowMetadata mockMetadata = mock(RowMetadata.class);
    when(mockRow1.generateMetadata()).thenReturn(mockMetadata);

    SpannerClientLibraryRow mockRow2 = mock(SpannerClientLibraryRow.class);
    when(mockRow2.get("col")).thenReturn("value2");

    SpannerClientLibraryResult result = new SpannerClientLibraryResult(Flux.just(mockRow1, mockRow2), 0);

    StepVerifier.create(
        result.map((r, rm) -> r.get("col"))
    ).expectNext("value1", "value2")
    .verifyComplete();

    verify(mockRow1).generateMetadata();
    verify(mockRow2, times(0)).generateMetadata();
  }



}

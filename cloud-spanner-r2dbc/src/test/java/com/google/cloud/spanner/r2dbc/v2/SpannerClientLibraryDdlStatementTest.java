package com.google.cloud.spanner.r2dbc.v2;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class SpannerClientLibraryDdlStatementTest {

  DatabaseClientReactiveAdapter mockClientLibraryAdapter;

  @BeforeEach
  void setUp() {
    this.mockClientLibraryAdapter = mock(DatabaseClientReactiveAdapter.class);
    when(this.mockClientLibraryAdapter.runDdlStatement(anyString()))
        .thenReturn(Mono.empty());
  }

  @Test
  void parameterBindingNotSupportedInDdl() {
    SpannerClientLibraryDdlStatement statement =
        new SpannerClientLibraryDdlStatement("unused", this.mockClientLibraryAdapter);
    assertThrows(UnsupportedOperationException.class,
        () -> statement.bind(1, "val"));
    assertThrows(UnsupportedOperationException.class,
        () -> statement.bind("col", "val"));
    assertThrows(UnsupportedOperationException.class,
        () -> statement.bindNull(1, Long.class));
    assertThrows(UnsupportedOperationException.class,
        () -> statement.bindNull("col", Long.class));
  }

  @Test
  void executeDdlAffectsZeroRows() {
    SpannerClientLibraryDdlStatement statement =
        new SpannerClientLibraryDdlStatement("unused", this.mockClientLibraryAdapter);

    StepVerifier.create(
        statement.execute().flatMap((Result r) -> Mono.from(r.getRowsUpdated()))
    ).expectNext(0)
        .verifyComplete();
  }

}

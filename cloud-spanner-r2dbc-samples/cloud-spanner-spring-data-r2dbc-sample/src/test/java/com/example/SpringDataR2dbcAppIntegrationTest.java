package com.example;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.ServiceOptions;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class SpringDataR2dbcAppIntegrationTest {

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    registry.add("gcp.project", () -> System.getProperty("gcp.project", ServiceOptions.getDefaultProjectId()));
    registry.add("spanner.database", () -> System.getProperty("spanner.database","testdb"));
    registry.add("spanner.instance", () -> System.getProperty("spanner.instance", "reactivetest"));
  }

  @Autowired
  private WebTestClient webTestClient;

  @AfterEach
  void deleteRecords() {
    this.webTestClient.get().uri("delete-all").exchange().expectStatus().is2xxSuccessful();
  }

  @Test
  void testBasicWebEndpoints() {

    // initially empty table
    this.webTestClient.get().uri("/list").exchange()
        .expectBody(Book[].class).isEqualTo(new Book[0]);

    this.webTestClient.post().uri("/add").body(Mono.just("Call of the wild"), String.class)
        .exchange().expectStatus().is2xxSuccessful();

    AtomicReference<String> id = new AtomicReference<>();
    this.webTestClient.get().uri("/list").exchange()
        .expectBody(Book[].class).value(books -> {
       assertThat(books).hasSize(1);
       assertThat(books[0].getTitle()).isEqualTo("Call of the wild");
       id.set(books[0].getId());
    });

    assertThat(id).doesNotHaveValue("");

    this.webTestClient.get().uri("/search/"  +id.get()).exchange()
        .expectBody(Book.class).value(book -> {
      assertThat(book.getTitle()).isEqualTo("Call of the wild");
    });
  }

  @Test
  void testJsonWebEndpoints() {
    this.webTestClient.post().uri("/add-json").header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(Mono.just("{\"input1\" : \"Call of the wild II\", \"input2\" : \"8\", \"input3\" : \"yes\"}"), String.class)
            .exchange().expectStatus().is2xxSuccessful();

    this.webTestClient.post().uri("/add-json-custom-class").header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .body(Mono.just("{\"input1\" : \"Call of the wild III\", \"input2\" : \"John\", \"input3\" : \"Good read.\"}"), String.class)
            .exchange().expectStatus().is2xxSuccessful();

    this.webTestClient
        .get()
        .uri("/list")
        .exchange()
        .expectBody(Book[].class)
        .value(
            books -> {
              assertThat(books).hasSize(2);
              for (Book book : books) {
                if (book.getTitle().equals("Call of the wild II")) {
                  assertThat(book.getExtraDetails()).containsEntry("rating", "8");
                  assertThat(book.getExtraDetails()).containsEntry("series", "yes");
                }
                if (book.getTitle().equals("Call of the wild III")) {
                  assertThat(book.getReview()).isEqualTo(new Review("John", "Good read."));
                }
              }
            });
  }

}

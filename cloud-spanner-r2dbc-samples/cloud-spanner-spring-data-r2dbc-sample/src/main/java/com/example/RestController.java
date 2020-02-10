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

package com.example;

import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 */
@org.springframework.web.bind.annotation.RestController
public class RestController {

  @Autowired
  private DatabaseClient r2dbcClient;

  @GetMapping("/list")
  public Flux<Book> listBooks() {
    return r2dbcClient.execute("SELECT id, title FROM BOOK")
        .as(Book.class)
        .fetch().all();
     //   .map(b -> b.getTitle());

    /*
    this works
    r2dbcClient.execute("SELECT title FROM BOOK")
        .map(r -> (String)r.get(0))
        .all();
        */
  }


  @PostMapping("/add")
  public Mono<Void> addBook(@RequestBody String bookTitle) {
    return r2dbcClient.insert()
        .into("book")
        .value("id", UUID.randomUUID().toString())
        .value("title", bookTitle)
        .then().then(Mono.fromRunnable(() -> {
          System.out.println("WRITING VALUE: " + bookTitle);
        }));
  }

}

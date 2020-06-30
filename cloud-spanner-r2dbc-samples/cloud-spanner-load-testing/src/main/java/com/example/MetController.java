package com.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/artworks")
public class MetController {

  @GetMapping("/client-library")
  Flux<String> getArtworksClientLibrary() {
    return Flux.just("statue - client library", "painting - client library");
  }

  @GetMapping("/r2dbc-grpc")
  Flux<String> getArtworksR2dbcGrpc() {
    return Flux.just("statue - r2dbc-grpc", "painting - r2dbc-grpc");
  }

  @GetMapping("/r2dbc-clientlibrary")
  Flux<String> getArtworksR2dbcClientLibrary() {
    return Flux.just("statue - r2dbc-clientlibrary", "painting - r2dbc-clientlibrary");
  }

}

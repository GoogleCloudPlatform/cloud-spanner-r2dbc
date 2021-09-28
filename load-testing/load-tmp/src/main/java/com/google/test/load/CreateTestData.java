package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CreateTestData {

  final static DatabaseClient dbClient;

  static {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    DatabaseId db = DatabaseId.of(options.getProjectId(), "loadtest", "store");
    dbClient = spanner.getDatabaseClient(db);
  }

  public static void main(String[] args) {
    /*
    CREATE TABLE inventory (
  uuid STRING(36),
  product_id INT64,
  product_count INT64
) PRIMARY KEY (uuid);
    */


    int numRows = 1_000_000;
    int batchSize = 1_000;

    int productId = 1;

    for (int i = 0; i < numRows / batchSize; i++) {
      System.out.println("Writing batch " + i);

      System.out.println("old product id = " + productId);
      productId = writeBatch(productId, batchSize);
      System.out.println("new product id = " + productId);
    }
  }

  private static int writeBatch(int startProductId, int batchSize) {
    int productId = startProductId;

    List<Mutation> mutations = new ArrayList<>();

    for (int i = 0; i < batchSize; i++) {
      mutations.add(
          Mutation.newInsertBuilder("inventory")
              .set("uuid")
              .to(UUID.randomUUID().toString())
              .set("product_id")
              .to(productId)
              .set("product_count")
              .to(10)
              .build());
      productId++;
    }

    dbClient.write(mutations);

    return productId;
  }
}

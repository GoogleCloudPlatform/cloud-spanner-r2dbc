package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import java.util.Random;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebController {
  private static final int MAX_PRODUCT_ID = 1_000_000;
  private static Random rand = new Random();

  @Autowired
  private DatabaseClient dbClient;

  @PostMapping("/update/{count}")
  private void updateAnyProduct(@PathVariable int count) {
    int randomProductId = rand.nextInt(MAX_PRODUCT_ID);
    System.out.println("Updating product ID " + randomProductId);

    dbClient
        .readWriteTransaction()
        .run(ctx ->  {
          Thread.sleep(1000);

          // simulate 1s of business processing inside of transaction
          ctx.executeUpdate(
                  Statement.newBuilder("UPDATE inventory SET product_count=@count WHERE product_id=@product_id")
                      .bind("count")
                      .to(count)
                      .bind("product_id")
                      .to(randomProductId)
                      .build());

          return null;
        });
  }
}

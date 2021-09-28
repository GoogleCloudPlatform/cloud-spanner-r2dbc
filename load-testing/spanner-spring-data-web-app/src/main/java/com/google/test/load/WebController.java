package com.google.test.load;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Random;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebController {
  private static final int MAX_PRODUCT_ID = 1_000_000;
  private static Random rand = new Random();

  @Autowired
  private InventoryRepository inventoryRepository;

  //@Autowired
  private Spanner spanner;
  private DatabaseClient dbClient;
/*

  @Transactional
  @PostMapping("/update/{count}")
  public void updateAnyProduct(@PathVariable int count) throws Exception {
    int randomProductId = rand.nextInt(MAX_PRODUCT_ID);
    System.out.println("Updating product ID " + randomProductId);

    Thread.sleep(1000);
    inventoryRepository.updateInventory(count, randomProductId);
  }
*/

  public DatabaseClient getDatabaseClient() {
    try {
      if ((spanner != null && spanner.isClosed()) || dbClient == null) {
        // note: not using autowiring, so there are 2 Spanner objects hanging about
        System.out.println("Create new Spanner Database client");
        SpannerOptions options = SpannerOptions.newBuilder().build();
        spanner = options.getService();
        // Creates a database client
        dbClient = spanner.getDatabaseClient(DatabaseId.of("elfel-spring", "loadtest", "store"));
      } else {
        System.out.println("Use existing Spanner Database client");
      }
    } catch (Exception exception) {
      System.out.println("Error while creating Spanner DatabaseClient -> {}, Exception -> {}" +exception.getStackTrace());
    }
    return dbClient;
  }



  @PostMapping("/delete")
  public void deleteAtsInventoryStage() {
    TransactionRunner transactionRunner = getDatabaseClient().readWriteTransaction();
    final TransactionRunner.TransactionCallable<String> callable =
        (TransactionRunner.TransactionCallable<String>)
            new TransactionRunner.TransactionCallable<String>() {
              @Override
              public String run(TransactionContext transaction) throws Exception {
                deleteProcessedTransaction(transaction);
                return "whatever";
              }
            };
    transactionRunner.run(callable);
  }

  @GetMapping("/threaddump")
  public String  dumpThreadDump() {
    StringBuffer buffer = new StringBuffer();
    ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    for (ThreadInfo ti : threadMxBean.dumpAllThreads(true, true)) {
      buffer.append(ti.toString());
    }
    return buffer.toString();
  }

  private void deleteProcessedTransaction(TransactionContext transaction) {
    int randomProductId = rand.nextInt(110);
    //int randomProductId = 45;
    String query = " DELETE FROM inventory_to_delete WHERE product_id IN (" + randomProductId +
        ") AND uuid IN (SELECT DISTINCT uuid FROM inventory_to_delete WHERE product_id IN (" + randomProductId
        + ") LIMIT 5000 )";

    // String query = " DELETE FROM Table1 WHERE LOCATION IN ('" + value1 +"') AND value IN (SELECT
    // DISTINCT value FROM Table1 WHERE LOCATION IN ('" + value1 +"') LIMIT "+value2+" )";
    //        transaction.executeUpdate(Statement.of(query));
    long before = System.currentTimeMillis();
    transaction.executeUpdate(Statement.of(query));
    long after = System.currentTimeMillis();
    long timeInSeconds = (after - before) / 1000;

    System.out.println("Deleting 5000 records took " + timeInSeconds + " seconds (" + (after - before) + " ms)");
  }


}

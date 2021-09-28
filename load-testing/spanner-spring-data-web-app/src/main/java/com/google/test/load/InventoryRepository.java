package com.google.test.load;

import com.google.cloud.spring.data.spanner.repository.SpannerRepository;
import com.google.cloud.spring.data.spanner.repository.query.Query;
import org.springframework.data.repository.query.Param;

public interface InventoryRepository extends SpannerRepository<Inventory, String> {

  @Query(value = "UPDATE inventory SET product_count=@count WHERE product_id=@product_id", dmlStatement = true)
  public long updateInventory(int count, @Param("product_id") int productId);

}

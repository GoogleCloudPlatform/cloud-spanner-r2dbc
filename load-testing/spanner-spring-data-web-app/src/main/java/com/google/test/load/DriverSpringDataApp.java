package com.google.test.load;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
//@EnableTransactionManagement
public class DriverSpringDataApp {
  public static void main(String[] args) {
    SpringApplication.run(DriverSpringDataApp.class, args);
  }
}

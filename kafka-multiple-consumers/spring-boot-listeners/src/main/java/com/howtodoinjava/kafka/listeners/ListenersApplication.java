package com.howtodoinjava.kafka.listeners;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class ListenersApplication {

  public static void main(String[] args) {
    SpringApplication.run(ListenersApplication.class, args);
  }

  @Bean
  NewTopic ordersTopic() {
    return TopicBuilder.name("orders").partitions(3).replicas(1).build();
  }

  @Bean
  ApplicationRunner sendOrders(KafkaTemplate<String, String> template) {
    return args -> {
      for (int i = 1; i <= 6; i++) {
        template.send("orders", "order-" + i, "amount " + (i * 10)).get();
      }
    };
  }
}

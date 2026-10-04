package com.howtodoinjava.kafka.listeners;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListeners {

  private static final Logger log = LoggerFactory.getLogger(OrderListeners.class);

  // listener id -> keys it received, for the tests
  final Map<String, List<String>> received = new ConcurrentHashMap<>();

  // 3 consumers in the group "billing": each one runs on its own thread
  @KafkaListener(id = "billing", topics = "orders", groupId = "billing", concurrency = "3")
  void billing(ConsumerRecord<String, String> rec) {
    log.info("billing  read {} from partition {}", rec.key(), rec.partition());
    received.computeIfAbsent("billing", k -> new CopyOnWriteArrayList<>()).add(rec.key());
  }

  // a second group: it receives every order again
  @KafkaListener(id = "shipping", topics = "orders", groupId = "shipping")
  void shipping(ConsumerRecord<String, String> rec) {
    log.info("shipping read {} from partition {}", rec.key(), rec.partition());
    received.computeIfAbsent("shipping", k -> new CopyOnWriteArrayList<>()).add(rec.key());
  }
}

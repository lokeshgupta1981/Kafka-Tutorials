package com.howtodoinjava.kafka.listeners;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * Checks the two listeners. The subclasses start Kafka and choose the group protocol.
 */
abstract class AbstractOrderListenersTest {

  @Autowired
  OrderListeners listeners;

  @Autowired
  KafkaListenerEndpointRegistry registry;

  @Test
  void billingSplitsTheOrdersAndShippingGetsAllOfThem() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (size("billing") < 6 || size("shipping") < 6) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out: " + listeners.received);
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    assertEquals(6, size("billing"));
    assertEquals(6, size("shipping"));

    // concurrency = "3" created 3 KafkaConsumers; each one ends up with 1 of the 3 partitions.
    // With group.protocol=consumer the broker moves partitions on the next heartbeats (every 5 s).
    var billing = (ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer("billing");
    List<Integer> perConsumer = List.of();
    while (!perConsumer.equals(List.of(1, 1, 1)) && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(200);
      perConsumer = billing.getContainers().stream()
          .map(c -> c.getAssignedPartitions().size())
          .toList();
    }
    assertEquals(List.of(1, 1, 1), perConsumer);

    var shipping = registry.getListenerContainer("shipping");
    assertEquals(3, shipping.getAssignedPartitions().size());
  }

  private int size(String id) {
    return listeners.received.getOrDefault(id, List.of()).size();
  }
}

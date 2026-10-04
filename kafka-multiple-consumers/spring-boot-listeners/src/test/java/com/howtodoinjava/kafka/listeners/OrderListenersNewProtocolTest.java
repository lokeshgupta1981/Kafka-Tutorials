package com.howtodoinjava.kafka.listeners;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

// the same listeners with the new consumer group protocol (KIP-848)
@SpringBootTest(properties = "spring.kafka.consumer.properties[group.protocol]=consumer")
@Testcontainers
class OrderListenersNewProtocolTest extends AbstractOrderListenersTest {

  @Container
  @ServiceConnection
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
}

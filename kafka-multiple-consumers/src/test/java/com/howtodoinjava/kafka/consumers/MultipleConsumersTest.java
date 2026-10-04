package com.howtodoinjava.kafka.consumers;

import static com.howtodoinjava.kafka.consumers.ConsumerGroups.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class MultipleConsumersTest {

  @Container
  static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

  private static final Duration WAIT = Duration.ofSeconds(60);
  private static final AtomicInteger RUN = new AtomicInteger();

  private String newTopic() throws Exception {
    String topic = "orders-" + RUN.incrementAndGet();
    KafkaSettings.recreateTopic(KAFKA.getBootstrapServers(), topic, 3);
    return topic;
  }

  @ParameterizedTest(name = "{0} consumers, protocol {1}")
  @CsvSource({
      "1, classic,  3",
      "2, classic,  2;1",
      "3, classic,  1;1;1",
      "4, classic,  1;1;1;0",
      "3, consumer, 1;1;1",
      "4, consumer, 1;1;1;0"})
  void consumersInOneGroupSplitThePartitions(int count, String protocol, String expected)
      throws Exception {
    String topic = newTopic();
    String bootstrap = KAFKA.getBootstrapServers();
    List<OrderConsumer> group = startGroup(bootstrap, topic, "billing-" + topic, count, protocol);
    try {
      awaitStableAssignment(group, 3, WAIT);
      List<String> sizes = group.stream()
          .map(c -> c.partitions().size())
          .sorted(Comparator.reverseOrder())
          .map(String::valueOf)
          .toList();
      assertEquals(expected, String.join(";", sizes));

      sendOrders(bootstrap, topic, 1, 6);
      await(() -> totalReceived(group) == 6, WAIT);
      // every order was read by exactly one consumer of the group
      List<String> keys = group.stream().flatMap(c -> c.receivedKeys().stream()).sorted().toList();
      assertEquals(List.of("order-1", "order-2", "order-3", "order-4", "order-5", "order-6"), keys);
    } finally {
      stopAll(group);
    }
  }

  @Test
  void everyGroupReceivesEveryMessage() throws Exception {
    String topic = newTopic();
    String bootstrap = KAFKA.getBootstrapServers();
    List<OrderConsumer> billing = startGroup(bootstrap, topic, "billing-" + topic, 2, "classic");
    List<OrderConsumer> shipping = startGroup(bootstrap, topic, "shipping-" + topic, 1, "classic");
    try {
      awaitStableAssignment(billing, 3, WAIT);
      awaitStableAssignment(shipping, 3, WAIT);
      sendOrders(bootstrap, topic, 1, 6);
      await(() -> totalReceived(billing) == 6 && totalReceived(shipping) == 6, WAIT);
      assertEquals(Set.of(0, 1, 2), shipping.getFirst().partitions());
      assertEquals(6, shipping.getFirst().received().size());
    } finally {
      stopAll(billing);
      stopAll(shipping);
    }
  }

  @ParameterizedTest(name = "protocol {0}")
  @ValueSource(strings = {"classic", "cooperative", "consumer"})
  void remainingConsumersTakeOverAfterOneLeaves(String protocol) throws Exception {
    String topic = newTopic();
    String bootstrap = KAFKA.getBootstrapServers();
    List<OrderConsumer> group = startGroup(bootstrap, topic, "billing-" + topic, 3, protocol);
    try {
      awaitStableAssignment(group, 3, WAIT);
      group.removeLast().stop();
      awaitStableAssignment(group, 3, WAIT);

      Set<Integer> all = new HashSet<>();
      group.forEach(c -> all.addAll(c.partitions()));
      assertEquals(Set.of(0, 1, 2), all);

      sendOrders(bootstrap, topic, 1, 6);
      await(() -> totalReceived(group) == 6, WAIT);
    } finally {
      stopAll(group);
    }
  }

  @ParameterizedTest(name = "protocol {0}")
  @ValueSource(strings = {"cooperative", "consumer"})
  void incrementalRebalanceKeepsPartitionsOfRemainingConsumers(String protocol) throws Exception {
    String topic = newTopic();
    String bootstrap = KAFKA.getBootstrapServers();
    List<OrderConsumer> group = startGroup(bootstrap, topic, "billing-" + topic, 3, protocol);
    try {
      awaitStableAssignment(group, 3, WAIT);
      Set<Integer> first = group.get(0).partitions();
      Set<Integer> second = group.get(1).partitions();
      group.removeLast().stop();
      awaitStableAssignment(group, 3, WAIT);

      // no partition moves away from a consumer that stays in the group
      assertTrue(group.get(0).partitions().containsAll(first));
      assertTrue(group.get(1).partitions().containsAll(second));
    } finally {
      stopAll(group);
    }
  }

  @Test
  void newProtocolRejectsClientSideAssignor() {
    Properties props = KafkaSettings.consumer(KAFKA.getBootstrapServers(), "g", "consumer");
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
    ConfigException e = assertThrows(ConfigException.class, () -> new KafkaConsumer<>(props));
    assertEquals("partition.assignment.strategy cannot be set when group.protocol=CONSUMER",
        e.getMessage());
  }
}

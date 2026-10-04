package com.howtodoinjava.kafka.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Helpers that start consumers, wait for a stable assignment and send orders.
 */
public final class ConsumerGroups {

  private ConsumerGroups() {
  }

  /**
   * Starts 'count' consumers named billing-1, billing-2, ... in the same group.
   */
  public static List<OrderConsumer> startGroup(String bootstrap, String topic, String groupId,
      int count, String protocol) {
    List<OrderConsumer> consumers = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      consumers.add(
          new OrderConsumer(groupId + "-" + i, bootstrap, topic, groupId, protocol).start());
    }
    return consumers;
  }

  /**
   * Waits until every partition is owned by exactly one consumer of the group, the partitions are
   * spread evenly, and the assignment has not changed for one second.
   */
  public static void awaitStableAssignment(List<OrderConsumer> group, int partitionCount,
      Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    String last = "";
    long stableSince = System.nanoTime();
    while (System.nanoTime() < deadline) {
      String now = group.stream().map(c -> c.partitions().toString()).toList().toString();
      if (!now.equals(last)) {
        last = now;
        stableSince = System.nanoTime();
      } else if (coversAll(group, partitionCount)
          && System.nanoTime() - stableSince > TimeUnit.SECONDS.toNanos(1)) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    throw new IllegalStateException("No stable assignment: " + last);
  }

  private static boolean coversAll(List<OrderConsumer> group, int partitionCount) {
    Set<Integer> seen = new HashSet<>();
    int total = 0;
    for (OrderConsumer c : group) {
      seen.addAll(c.partitions());
      total += c.partitions().size();
    }
    int expectedMax = (partitionCount + group.size() - 1) / group.size();
    boolean even = group.stream().allMatch(c -> c.partitions().size() <= expectedMax);
    return seen.size() == partitionCount && total == partitionCount && even;
  }

  /**
   * Sends the orders with keys from..to (order-1, order-2, ...) and prints their partitions.
   */
  public static void sendOrders(String bootstrap, String topic, int from, int to)
      throws Exception {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(KafkaSettings.producer(bootstrap))) {
      for (int i = from; i <= to; i++) {
        String key = "order-" + i;
        RecordMetadata meta = producer.send(
            new ProducerRecord<>(topic, key, "amount " + (i * 10))).get();
        System.out.printf("sent %s to partition %d%n", key, meta.partition());
      }
    }
  }

  public static void await(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException("Timed out");
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
  }

  public static int totalReceived(List<OrderConsumer> consumers) {
    return consumers.stream().mapToInt(c -> c.received().size()).sum();
  }

  public static void stopAll(List<OrderConsumer> consumers) throws InterruptedException {
    for (OrderConsumer c : consumers) {
      c.stop();
    }
  }
}

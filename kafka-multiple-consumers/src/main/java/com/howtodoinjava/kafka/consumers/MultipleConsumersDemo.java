package com.howtodoinjava.kafka.consumers;

import static com.howtodoinjava.kafka.consumers.ConsumerGroups.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs the examples of the article against Kafka on localhost:9092.
 *
 * <pre>
 * mvn -q compile exec:exec -Ddemo="group 3"           # 3 consumers in one group
 * mvn -q compile exec:exec -Ddemo=groups               # two groups, both get every order
 * mvn -q compile exec:exec -Ddemo=rebalance            # billing-3 leaves the group
 * mvn -q compile exec:exec -Ddemo="group 3 consumer"  # same, with group.protocol=consumer
 * </pre>
 */
public class MultipleConsumersDemo {

  private static final Duration WAIT = Duration.ofSeconds(60);

  public static void main(String[] input) throws Exception {
    // exec:exec passes -Ddemo="group 3" as one argument
    String[] args = String.join(" ", input).trim().split("\\s+");
    String mode = args.length > 0 ? args[0] : "group";
    String bootstrap = KafkaSettings.BOOTSTRAP;
    String topic = KafkaSettings.TOPIC;
    KafkaSettings.recreateTopic(bootstrap, topic, 3);

    switch (mode) {
      case "group" -> {
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        String protocol = args.length > 2 ? args[2] : "classic";
        oneGroup(bootstrap, topic, count, protocol);
      }
      case "groups" -> twoGroups(bootstrap, topic);
      case "watch" -> watch(bootstrap, topic, Integer.parseInt(args[1]),
          args.length > 2 ? args[2] : "classic");
      case "rebalance" -> rebalance(bootstrap, topic, args.length > 1 ? args[1] : "classic");
      default -> throw new IllegalArgumentException("Unknown mode " + mode);
    }
  }

  static void oneGroup(String bootstrap, String topic, int count, String protocol)
      throws Exception {
    List<OrderConsumer> billing = startGroup(bootstrap, topic, "billing", count, protocol);
    awaitStableAssignment(billing, 3, WAIT);
    sendOrders(bootstrap, topic, 1, 6);
    await(() -> totalReceived(billing) == 6, WAIT);
    printSummary(billing);
    stopAll(billing);
  }

  static void twoGroups(String bootstrap, String topic) throws Exception {
    List<OrderConsumer> billing = startGroup(bootstrap, topic, "billing", 2, "classic");
    List<OrderConsumer> shipping = startGroup(bootstrap, topic, "shipping", 1, "classic");
    awaitStableAssignment(billing, 3, WAIT);
    awaitStableAssignment(shipping, 3, WAIT);
    sendOrders(bootstrap, topic, 1, 6);
    await(() -> totalReceived(billing) == 6 && totalReceived(shipping) == 6, WAIT);
    List<OrderConsumer> all = new ArrayList<>(billing);
    all.addAll(shipping);
    printSummary(all);
    stopAll(all);
  }

  static void rebalance(String bootstrap, String topic, String protocol) throws Exception {
    List<OrderConsumer> billing = startGroup(bootstrap, topic, "billing", 3, protocol);
    awaitStableAssignment(billing, 3, WAIT);
    printSummary(billing);

    System.out.println("--- stopping billing-3 ---");
    OrderConsumer leaving = billing.removeLast();
    leaving.stop();
    awaitStableAssignment(billing, 3, WAIT);
    printSummary(billing);

    sendOrders(bootstrap, topic, 1, 6);
    await(() -> totalReceived(billing) == 6, WAIT);
    printSummary(billing);
    stopAll(billing);
  }

  /**
   * Keeps 'count' consumers running for 60 seconds, so that kafka-consumer-groups.sh can show them.
   */
  static void watch(String bootstrap, String topic, int count, String protocol) throws Exception {
    List<OrderConsumer> billing = startGroup(bootstrap, topic, "billing", count, protocol);
    awaitStableAssignment(billing, 3, WAIT);
    printSummary(billing);
    TimeUnit.SECONDS.sleep(60);
    stopAll(billing);
  }

  static void printSummary(List<OrderConsumer> consumers) {
    System.out.println("--- summary ---");
    for (OrderConsumer c : consumers) {
      System.out.printf("%-10s partitions %-9s orders %s%n",
          c.name(), c.partitions(), c.receivedKeys());
    }
  }
}

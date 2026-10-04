package com.howtodoinjava.kafka.consumers;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * One consumer in a consumer group. Each OrderConsumer owns one KafkaConsumer and one thread,
 * because KafkaConsumer is not thread-safe.
 */
public class OrderConsumer implements Runnable {

  private final String name;
  private final String topic;
  private final KafkaConsumer<String, String> consumer;
  private final List<ConsumerRecord<String, String>> received = new CopyOnWriteArrayList<>();
  private volatile Set<Integer> partitions = Set.of();
  private Thread thread;

  public OrderConsumer(String name, String bootstrap, String topic, String groupId,
      String protocol) {
    this.name = name;
    this.topic = topic;
    this.consumer = new KafkaConsumer<>(KafkaSettings.consumer(bootstrap, groupId, protocol));
  }

  public OrderConsumer start() {
    thread = Thread.ofPlatform().name(name).start(this);
    return this;
  }

  @Override
  public void run() {
    consumer.subscribe(Set.of(topic), new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
        if (!revoked.isEmpty()) {
          System.out.printf("%s revoked  %s%n", name, numbers(revoked));
        }
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
        System.out.printf("%s assigned %s%n", name, numbers(assigned));
        // assigned holds only the new partitions with the cooperative protocols
        Set<Integer> now = new TreeSet<>();
        consumer.assignment().forEach(tp -> now.add(tp.partition()));
        partitions = now;
      }

      @Override
      public void onPartitionsLost(Collection<TopicPartition> lost) {
        onPartitionsRevoked(lost);
      }
    });
    try {
      while (true) {
        for (ConsumerRecord<String, String> rec : consumer.poll(Duration.ofMillis(100))) {
          received.add(rec);
          System.out.printf("%s read %s from partition %d%n", name, rec.key(), rec.partition());
        }
        // keep the partitions up to date after a revocation
        Set<Integer> now = new TreeSet<>();
        consumer.assignment().forEach(tp -> now.add(tp.partition()));
        partitions = now;
      }
    } catch (WakeupException e) {
      // stop() was called
    } finally {
      consumer.close();   // leaves the group, which starts a rebalance for the others
    }
  }

  /**
   * Stops the poll loop from another thread. wakeup() is the only thread-safe KafkaConsumer method.
   */
  public void stop() throws InterruptedException {
    consumer.wakeup();
    thread.join();
  }

  public String name() {
    return name;
  }

  public Set<Integer> partitions() {
    return partitions;
  }

  public List<ConsumerRecord<String, String>> received() {
    return received;
  }

  public List<String> receivedKeys() {
    return received.stream().map(ConsumerRecord::key).toList();
  }

  private static Set<Integer> numbers(Collection<TopicPartition> tps) {
    Set<Integer> result = new TreeSet<>();
    tps.forEach(tp -> result.add(tp.partition()));
    return result;
  }
}

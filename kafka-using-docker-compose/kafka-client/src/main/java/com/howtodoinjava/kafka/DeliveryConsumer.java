package com.howtodoinjava.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class DeliveryConsumer implements AutoCloseable {

  private final KafkaConsumer<String, String> consumer;

  public DeliveryConsumer(String groupId) {
    consumer = new KafkaConsumer<>(KafkaConfig.consumerProps(groupId));
    consumer.subscribe(Set.of(KafkaConfig.TOPIC));
  }

  // Polls until 'expected' records with one of the given keys arrive, or maxWait ends
  public List<ConsumerRecord<String, String>> read(Collection<String> keys, int expected,
      Duration maxWait) {
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    long deadline = System.currentTimeMillis() + maxWait.toMillis();
    while (records.size() < expected && System.currentTimeMillis() < deadline) {
      for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
        if (r.key() != null && keys.contains(r.key())) {
          records.add(r);
        }
      }
    }
    return records;
  }

  @Override
  public void close() {
    consumer.close();
  }
}

package com.howtodoinjava.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

// Needs the 3-node cluster from ../docker-compose.yaml and the "deliveries" topic
class DeliveryClusterTest {

  private final Admin admin = Admin.create(Map.of(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS));

  @Test
  void clusterHasThreeNodes() throws Exception {
    assertEquals(3, admin.describeCluster().nodes().get().size());
  }

  @Test
  void topicHasThreeReplicasPerPartition() throws Exception {
    TopicDescription topic = admin.describeTopics(List.of(KafkaConfig.TOPIC))
        .allTopicNames().get().get(KafkaConfig.TOPIC);
    assertEquals(3, topic.partitions().size());
    topic.partitions().forEach(p -> assertEquals(3, p.replicas().size()));
  }

  @Test
  void sameKeyGoesToSamePartitionAndIsReadBack() throws Exception {
    String parcel = "parcel-" + UUID.randomUUID();
    RecordMetadata first;
    RecordMetadata second;
    try (DeliveryProducer producer = new DeliveryProducer()) {
      first = producer.send(parcel, "picked up");
      second = producer.send(parcel, "delivered");
    }
    assertEquals(first.partition(), second.partition());
    assertEquals(first.offset() + 1, second.offset());

    try (DeliveryConsumer consumer = new DeliveryConsumer("test-" + UUID.randomUUID())) {
      List<String> statuses = consumer.read(List.of(parcel), 2, Duration.ofSeconds(30)).stream()
          .map(ConsumerRecord::value)
          .toList();
      assertEquals(List.of("picked up", "delivered"), statuses);
    }
  }
}

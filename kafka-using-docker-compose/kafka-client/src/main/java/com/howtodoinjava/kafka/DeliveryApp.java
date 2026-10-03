package com.howtodoinjava.kafka;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DeliveryApp {

  private static final List<String> PARCELS = List.of("parcel-4", "parcel-5", "parcel-6");

  public static void main(String[] args) throws Exception {
    try (Admin admin = Admin.create(java.util.Map.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS))) {
      var cluster = admin.describeCluster();
      System.out.println("Cluster " + cluster.clusterId().get()
          + ", nodes " + cluster.nodes().get().size());
    }

    try (DeliveryProducer producer = new DeliveryProducer()) {
      for (String parcel : PARCELS) {
        RecordMetadata meta = producer.send(parcel, "out for delivery");
        System.out.println("Sent " + parcel + " to partition " + meta.partition()
            + " at offset " + meta.offset());
      }
    }

    try (DeliveryConsumer consumer = new DeliveryConsumer("app-" + UUID.randomUUID())) {
      for (ConsumerRecord<String, String> r : consumer.read(PARCELS, 3, Duration.ofSeconds(30))) {
        System.out.println("Read " + r.key() + "=" + r.value() + " from partition " + r.partition());
      }
    }
  }
}

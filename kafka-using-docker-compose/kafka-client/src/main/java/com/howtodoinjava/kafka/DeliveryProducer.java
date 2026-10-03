package com.howtodoinjava.kafka;

import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DeliveryProducer implements AutoCloseable {

  private final KafkaProducer<String, String> producer =
      new KafkaProducer<>(KafkaConfig.producerProps());

  public RecordMetadata send(String parcel, String status)
      throws ExecutionException, InterruptedException {
    ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.TOPIC, parcel, status);
    return producer.send(record).get();
  }

  @Override
  public void close() {
    producer.close();
  }
}

package com.howtodoinjava.avro.example.producer;

import com.howtodoinjava.avro.example.domain.generated.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class OrderProducer {

  private static final String ORDERS_TOPIC_SR = "orders-sr";

  public static void main(String[] args) {

    KafkaProducer<String, Order> producer = configureProducer();
    Order order = buildNewOrder();

    ProducerRecord<String, Order> producerRecord =
        new ProducerRecord<>(ORDERS_TOPIC_SR, order.getId().toString(), order);

    producer.send(producerRecord, (metadata, exception) -> {
      if (exception == null) {
        System.out.println("Message produced, record metadata: " + metadata);
        System.out.println("Producing message with data: " + producerRecord.value());
      } else {
        System.err.println("Error producing message: " + exception.getMessage());
      }
    });

    producer.flush();
    producer.close();
  }

  private static KafkaProducer<String, Order> configureProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    return new KafkaProducer<>(properties);
  }

  private static Order buildNewOrder() {
    return Order.newBuilder()
        .setId(UUID.randomUUID())
        .setFirstName("John")
        .setLastName("Doe")
        //.setMiddleName("TestMiddleName")
        .setOrderedTime(Instant.now())
        .setStatus("NEW")
        .build();
  }
}

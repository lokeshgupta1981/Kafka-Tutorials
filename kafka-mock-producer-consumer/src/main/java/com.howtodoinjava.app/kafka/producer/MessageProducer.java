package com.howtodoinjava.app.kafka.producer;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class MessageProducer {

  private final Producer<String, String> producer;

  public MessageProducer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.producer = new KafkaProducer<>(properties);
  }

  public Future<RecordMetadata> notifyNewOrderCreated(String orderId, String message) {
    try {
      ProducerRecord<String, String> pRecord = new ProducerRecord<>(KafkaConstants.TOPIC_CREATE_ORDER, orderId, message);
      return producer.send(pRecord);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}

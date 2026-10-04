package com.howtodoinjava.kafka.consumers;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Consumer, producer and admin settings used by the examples.
 */
public final class KafkaSettings {

  public static final String BOOTSTRAP = "localhost:9092";
  public static final String TOPIC = "orders";

  private KafkaSettings() {
  }

  /**
   * Settings of one consumer. All consumers with the same groupId share the partitions.
   *
   * @param protocol "classic" (the default protocol with the default RangeAssignor),
   *                 "cooperative" (classic protocol with the CooperativeStickyAssignor) or
   *                 "consumer" (the new consumer group protocol from KIP-848)
   */
  public static Properties consumer(String bootstrap, String groupId, String protocol) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    if (protocol.equals("cooperative")) {
      props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
      props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
          CooperativeStickyAssignor.class.getName());
    } else {
      props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
    }
    return props;
  }

  public static Properties producer(String bootstrap) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  /**
   * Deletes the topic if it exists and creates it again, so that every run starts empty.
   */
  public static void recreateTopic(String bootstrap, String topic, int partitions)
      throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    try (Admin admin = Admin.create(props)) {
      try {
        admin.deleteTopics(Set.of(topic)).all().get();
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          throw e;
        }
      }
      // Topic deletion finishes in the background; retry the creation until it succeeds
      for (int attempt = 1; ; attempt++) {
        try {
          admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
          return;
        } catch (ExecutionException e) {
          if (attempt == 30) {
            throw e;
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
      }
    }
  }
}

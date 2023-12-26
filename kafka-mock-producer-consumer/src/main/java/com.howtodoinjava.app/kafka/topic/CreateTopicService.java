package com.howtodoinjava.app.kafka.topic;


import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

public class CreateTopicService {
  public static void createTopic(String topicName) {
    Properties properties = new Properties();
    properties.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER
    );

    try (Admin admin = Admin.create(properties)) {
      int partitions = 3;
      short replicationFactor = 1;

      NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

      admin.createTopics(
          Collections.singleton(newTopic)
      );
    }
  }
}

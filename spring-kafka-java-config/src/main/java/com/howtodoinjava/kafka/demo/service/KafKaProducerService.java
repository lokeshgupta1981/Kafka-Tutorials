package com.howtodoinjava.kafka.demo.service;

import com.howtodoinjava.kafka.demo.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafKaProducerService {
  private static final Logger logger =
      LoggerFactory.getLogger(KafKaProducerService.class);

  //1. General topic with string payload

  @Value(value = "${general.topic.name}")
  private String topicName;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  //2. Topic with user object payload

  @Value(value = "${user.topic.name}")
  private String userTopicName;

  @Autowired
  private KafkaTemplate<String, User> userKafkaTemplate;

  public void sendMessage(String message) {
    CompletableFuture<SendResult<String, String>> future
        = this.kafkaTemplate.send(topicName, message);

    future.whenComplete((result, throwable) -> {

      if (throwable != null) {
        // handle failure
        logger.error("Unable to send message : " + message, throwable);
      } else {
        // handle success
        logger.info("Sent message: " + message + " with offset: " + result.getRecordMetadata().offset());
      }
    });
  }

  public void saveCreateUserLog(User user) {
    CompletableFuture<SendResult<String, User>> future
        = this.userKafkaTemplate.send(userTopicName, user);

    future.whenComplete((result, throwable) -> {
      if (throwable != null) {
        // handle failure
        logger.error("User created : " + user, throwable);
      } else {
        // handle success
        logger.info("User created: " + user + " with offset: " + result.getRecordMetadata().offset());
      }
    });
  }
}

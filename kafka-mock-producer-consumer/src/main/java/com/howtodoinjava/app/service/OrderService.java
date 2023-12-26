package com.howtodoinjava.app.service;

import com.howtodoinjava.app.kafka.producer.MessageProducer;

public class OrderService {

  MessageProducer messageProducer;

  public OrderService(MessageProducer messageProducer) {
    this.messageProducer = messageProducer;
  }


}

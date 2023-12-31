package com.howtodoinjava.app.service;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.producer.MessageProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class OrderService
{

    private final MessageProducer messageProducer;

    public OrderService(MessageProducer messageProducer)
    {
        this.messageProducer = messageProducer;
    }

    public Future<RecordMetadata> createOrder(long orderId, String orderInfo)
    {
        return this.messageProducer.notify(KafkaConstants.TOPIC_CREATE_ORDER, orderId, orderInfo);
    }

}

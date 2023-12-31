package com.howtodoinjava.app.db;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.consumer.MessageConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OrderDB
{
    private final ConcurrentHashMap<Long, String> orders = new ConcurrentHashMap<>();

    private final MessageConsumer consumer;

    private Thread consumerThread;

    public OrderDB(MessageConsumer consumer)
    {
        this.consumer = consumer;
    }

    public void startDB()
    {
        if (consumerThread == null)
        {
            consumerThread = new Thread(() -> consumer.startPolling(KafkaConstants.TOPIC_CREATE_ORDER, record ->
            {
                System.out.println("---------------------Message Received---------------------");
                System.out.println("Topic: " + record.topic());
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value());
                System.out.println("Partition: " + record.partition());
                System.out.println("Offset: " + record.offset());

                orders.put(record.key(), record.value());
            }));

            consumerThread.start();
        }

    }

    public void startDB(int partition)
    {
        if (consumerThread == null)
        {
            consumerThread = new Thread(() -> consumer.startPolling(KafkaConstants.TOPIC_CREATE_ORDER, partition, record ->
            {
                System.out.println("---------------------Message Received On Partition " + record.partition() + " ---------------------");
                System.out.println("Topic: " + record.topic());
                System.out.println("Key: " + record.key());
                System.out.println("Value: " + record.value());
                System.out.println("Offset: " + record.offset());

                orders.put(record.key(), record.value());
            }));

            consumerThread.start();
        }
    }

    public Map<Long, String> getOrders()
    {
        return Collections.unmodifiableMap(orders);
    }

    public void stopDB()
    {
        consumer.stopPolling();
    }
}

package com.howtodoinjava.app;

import com.howtodoinjava.app.db.OrderDB;
import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.consumer.MessageConsumer;
import com.howtodoinjava.app.kafka.producer.MessageProducer;
import com.howtodoinjava.app.kafka.topic.CreateTopicService;
import com.howtodoinjava.app.service.OrderService;

public class Main
{
    public static void main(String[] args)
    {

        CreateTopicService.createTopic(KafkaConstants.TOPIC_CREATE_ORDER);
        System.out.println("Topic created");

        try (var producer = new MessageProducer(); var consumer = new MessageConsumer())
        {
            System.out.println("MessageProducer and MessageConsumer created");

            OrderDB db = new OrderDB(consumer);
            db.startDB();

            OrderService orderService = new OrderService(producer);


            for (int i = 0; i < 10; i++)
            {
                orderService.createOrder(i, "Product " + i);

                Thread.sleep(1000);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(db::stopDB));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}

package com.howtodoinjava.app.kafka.producer;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class MessageProducer implements AutoCloseable
{

    private final Producer<Long, String> producer;

    public MessageProducer()
    {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
    }

    public MessageProducer(Producer<Long, String> producer)
    {
        this.producer = producer;
    }

    public Future<RecordMetadata> notify(String topic, Long orderId, String orderInfo)
    {
        try
        {
            var pRecord = new ProducerRecord<>(topic, orderId, orderInfo);
            return producer.send(pRecord);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception
    {
        producer.close();
    }
}

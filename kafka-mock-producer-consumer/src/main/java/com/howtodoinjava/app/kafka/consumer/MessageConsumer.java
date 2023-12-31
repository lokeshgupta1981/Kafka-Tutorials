package com.howtodoinjava.app.kafka.consumer;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageConsumer implements AutoCloseable
{
    private final Consumer<Long, String> consumer;

    private static final AtomicBoolean HAS_MORE_MESSAGES = new AtomicBoolean(true);

    java.util.function.Consumer<ConsumerRecord<Long, String>> action;

    public MessageConsumer()
    {
        var properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());

        this.consumer = new KafkaConsumer<>(properties);
    }

    public MessageConsumer(Consumer<Long, String> consumer)
    {
        this.consumer = consumer;
    }

    public void startPolling(String topic, java.util.function.Consumer<ConsumerRecord<Long, String>> action)
    {
        this.action = action;

        consumer.subscribe(List.of(topic));

        startPolling();
    }

    public void startPolling(String topic, int partition, java.util.function.Consumer<ConsumerRecord<Long, String>> action)
    {
        this.action = action;

        consumer.assign(List.of(new TopicPartition(topic, partition)));

        startPolling();
    }

    private void startPolling()
    {
        try (consumer)
        {
            while (HAS_MORE_MESSAGES.get())
            {
                var consumerRecord = consumer.poll(Duration.ofMillis(300));

                consumerRecord.forEach(action);

                // commits the offset
                consumer.commitAsync();
            }
        }
        catch (WakeupException ignored)
        {

        }
        catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
    }

    public void stopPolling()
    {
        System.out.println("Stopping Consumer");

        consumer.wakeup();

        HAS_MORE_MESSAGES.set(false);
    }

    @Override
    public void close() throws Exception
    {
        consumer.close();
    }
}

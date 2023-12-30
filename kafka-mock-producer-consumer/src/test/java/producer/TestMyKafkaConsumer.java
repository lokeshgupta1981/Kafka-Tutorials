package producer;

import com.howtodoinjava.app.db.OrderDB;
import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.consumer.MessageConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMyKafkaConsumer
{

    @Test
    void addOrder_verifyOrderStored()
    {
        try (var mockConsumer = new MockConsumer<Long, String>(OffsetResetStrategy.EARLIEST); var consumer = new MessageConsumer(mockConsumer))
        {
            OrderDB orderDB = new OrderDB(consumer);

            orderDB.startDB(1);

            mockConsumer.updateBeginningOffsets(Map.of(new TopicPartition(KafkaConstants.TOPIC_CREATE_ORDER, 1), 0L));

            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(KafkaConstants.TOPIC_CREATE_ORDER, 1, 0, 1L, "Product 1")));

            TimeUnit.MILLISECONDS.sleep(200); // wait for consumer to poll

            Assertions.assertEquals(1, orderDB.getOrders().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void startDBOnDifferentPartition_verifyNoOrderStored()
    {
        try (var mockConsumer = new MockConsumer<Long, String>(OffsetResetStrategy.EARLIEST); var consumer = new MessageConsumer(mockConsumer))
        {
            OrderDB orderDB = new OrderDB(consumer);

            orderDB.startDB(1);

            mockConsumer.updateBeginningOffsets(Map.of(new TopicPartition(KafkaConstants.TOPIC_CREATE_ORDER, 1), 0L));

            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(KafkaConstants.TOPIC_CREATE_ORDER, 2, 0, 1L, "Product 1")));

            TimeUnit.MILLISECONDS.sleep(200); // wait for consumer to poll

            Assertions.assertEquals(0, orderDB.getOrders().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void addOrderWithDifferentTopic_verifyNoOrderStored()
    {
        try (var mockConsumer = new MockConsumer<Long, String>(OffsetResetStrategy.EARLIEST); var consumer = new MessageConsumer(mockConsumer))
        {
            OrderDB orderDB = new OrderDB(consumer);

            orderDB.startDB(1);

            mockConsumer.updateBeginningOffsets(Map.of(new TopicPartition(KafkaConstants.TOPIC_CREATE_ORDER, 1), 0L));

            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(KafkaConstants.TOPIC_CREATE_ORDER + "-1", 2, 0, 1L, "Product 1")));

            TimeUnit.MILLISECONDS.sleep(200); // wait for consumer to poll

            Assertions.assertEquals(0, orderDB.getOrders().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void testStopOrderDB()
    {
        try (var mockConsumer = new MockConsumer<Long, String>(OffsetResetStrategy.EARLIEST); var consumer = new MessageConsumer(mockConsumer))
        {
            OrderDB orderDB = new OrderDB(consumer);

            orderDB.startDB(1);

            orderDB.stopDB();

            TimeUnit.MILLISECONDS.sleep(200); // wait for consumer to poll

            Assertions.assertTrue(mockConsumer.closed());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

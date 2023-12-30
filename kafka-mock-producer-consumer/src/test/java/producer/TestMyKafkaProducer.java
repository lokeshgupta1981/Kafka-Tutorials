package producer;

import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.producer.MessageProducer;
import com.howtodoinjava.app.service.OrderService;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestMyKafkaProducer
{
    @Test
    void createOrder_verifyOrderCreated()
    {
        try (var mockProducer = new MockProducer<>(true, new LongSerializer(), new StringSerializer()); var producer = new MessageProducer(mockProducer))
        {
            OrderService orderService = new OrderService(producer);

            orderService.createOrder(1, "Product 1");

            Assertions.assertEquals(1, mockProducer.history().size());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void createOrder_verifyTheTopic()
    {
        try (var mockProducer = new MockProducer<>(true, new LongSerializer(), new StringSerializer()); var producer = new MessageProducer(mockProducer))
        {
            OrderService orderService = new OrderService(producer);

            var metadata = orderService.createOrder(1, "Product 1");

            Assertions.assertEquals(1, mockProducer.history().size());

            Assertions.assertEquals(metadata.get().topic(), mockProducer.history().get(0).topic());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void createOrderWithDifferentOrderID_verifySentToDifferentTopicPartition()
    {
        PartitionInfo partitionInfo0 = new PartitionInfo(KafkaConstants.TOPIC_CREATE_ORDER, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(KafkaConstants.TOPIC_CREATE_ORDER, 1, null, null, null);

        List<PartitionInfo> list = new ArrayList<>();

        list.add(partitionInfo0);
        list.add(partitionInfo1);

        Cluster kafkaCluster = new Cluster("id1", new ArrayList<>(), list, Collections.emptySet(), Collections.emptySet());

        try (var mockProducer = new MockProducer<>(kafkaCluster, true, new LongSerializer(), new StringSerializer()); var producer = new MessageProducer(mockProducer))
        {
            OrderService orderService = new OrderService(producer);

            var metadata1 = orderService.createOrder(1, "Product 1");

            var metadata2 = orderService.createOrder(3, "Product 11");

            Assertions.assertNotEquals(metadata1.get().partition(), metadata2.get().partition());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void createOrder_raiseException_verifyException()
    {
        try (var mockProducer = new MockProducer<>(false, new LongSerializer(), new StringSerializer()); var producer = new MessageProducer(mockProducer))
        {
            OrderService orderService = new OrderService(producer);

            var metadata = orderService.createOrder(1, "Product 1");

            InvalidTopicException e = new InvalidTopicException();
            mockProducer.errorNext(e);

            try
            {
                metadata.get();
            }
            catch (Exception exception)
            {
                Assertions.assertEquals(e, exception.getCause());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    void createOrder_commitTransaction_verifyOrderCreated()
    {

        try (var mockProducer = new MockProducer<>(false, new LongSerializer(), new StringSerializer()); var producer = new MessageProducer(mockProducer))
        {
            OrderService orderService = new OrderService(producer);

            mockProducer.initTransactions();

            mockProducer.beginTransaction();

            var metadata = orderService.createOrder(1, "Product 1");

            Assertions.assertFalse(metadata.isDone());

            Assertions.assertEquals(mockProducer.history().size(), 0);

            mockProducer.commitTransaction();

            Assertions.assertEquals(mockProducer.history().size(), 1);

            Assertions.assertTrue(metadata.isDone());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}

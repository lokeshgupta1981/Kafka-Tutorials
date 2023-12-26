import com.howtodoinjava.app.kafka.constants.KafkaConstants;
import com.howtodoinjava.app.kafka.consumer.MessageConsumer;
import com.howtodoinjava.app.kafka.producer.MessageProducer;
import com.howtodoinjava.app.kafka.topic.CreateTopicService;

import java.util.concurrent.ExecutionException;

public class Main {
  public static void main(String[] args) throws InterruptedException, ExecutionException {

    CreateTopicService.createTopic(KafkaConstants.TOPIC_CREATE_ORDER);
    System.out.println("Topic created");
    MessageProducer producer = new MessageProducer();
    System.out.println("MessageProducer created");
    MessageConsumer consumer = new MessageConsumer();
    System.out.println("MessageConsumer created");
    Thread consumerThread = new Thread(() -> consumer.startPolling(KafkaConstants.TOPIC_CREATE_ORDER));

    consumerThread.start();

    for (int i = 0; i < 10; i++) {
      System.out.println("Message Sent to the topic: " +
          producer.notifyNewOrderCreated("Hello", "Hello World " + i).get().topic());
    }

    Runtime.getRuntime().addShutdownHook(new Thread(consumer::stopPolling));
    consumerThread.join();
  }
}

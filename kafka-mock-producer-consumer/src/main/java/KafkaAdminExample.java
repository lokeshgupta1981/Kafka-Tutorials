import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAdminExample {
  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    try (AdminClient admin = AdminClient.create(config)) {
      ListTopicsResult topics = admin.listTopics();
      topics.names().get().forEach(System.out::println);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }
}

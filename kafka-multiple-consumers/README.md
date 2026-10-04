# Kafka Multiple Consumers (Consumer Groups)

Source code for the article [Kafka Multiple Consumers: Consumer Groups and Partitions](https://howtodoinjava.com/kafka/multiple-consumers-example/).

## Versions

- Apache Kafka 4.3.1 (official image `apache/kafka:4.3.1`), KRaft mode
- Java 25, kafka-clients 4.3.1, JUnit 6.1.3, Testcontainers 2.0.5, Maven
- `spring-boot-listeners/`: Spring Boot 4.1.1 (Spring for Apache Kafka 4.1.1) with kafka-clients 4.3.1

## Files

| File | What it does |
|---|---|
| `docker-compose.yaml` | One Kafka node, client port `localhost:9092` |
| `KafkaSettings.java` | Consumer and producer settings; creates the `orders` topic with 3 partitions |
| `OrderConsumer.java` | One `KafkaConsumer` on its own thread, prints its partitions and the orders it reads |
| `ConsumerGroups.java` | Starts N consumers in a group, waits for a stable assignment, sends orders |
| `MultipleConsumersDemo.java` | The runs shown in the article |
| `MultipleConsumersTest.java` | Testcontainers tests for 1 to 4 consumers, two groups, rebalancing and both group protocols |
| `spring-boot-listeners/` | The same idea with `@KafkaListener(concurrency = "3")` and two group ids |

## Run the examples

```bash
docker compose up -d

mvn -q compile exec:exec -Ddemo="group 1"            # 1 consumer reads all 3 partitions
mvn -q compile exec:exec -Ddemo="group 3"            # 3 consumers, 1 partition each
mvn -q compile exec:exec -Ddemo="group 4"            # the 4th consumer stays idle
mvn -q compile exec:exec -Ddemo=groups               # billing and shipping both get every order
mvn -q compile exec:exec -Ddemo=rebalance            # billing-3 leaves, the others take over
mvn -q compile exec:exec -Ddemo="rebalance cooperative"   # CooperativeStickyAssignor
mvn -q compile exec:exec -Ddemo="rebalance consumer"      # group.protocol=consumer (KIP-848)
mvn -q compile exec:exec -Ddemo="watch 4"            # keeps 4 consumers up for 60 seconds

docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --describe --group billing --members

docker compose down -v
```

## Run the tests

The tests start their own Kafka container with Testcontainers, so only Docker is needed:

```bash
mvn test
cd spring-boot-listeners && mvn test
```

## Spring Boot version

```bash
docker compose up -d
cd spring-boot-listeners
mvn spring-boot:run
```

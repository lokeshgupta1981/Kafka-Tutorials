# Kafka Cluster Setup Using Docker Compose (KRaft)

Source code for the article [Kafka Single and Multi-Node Clusters using Docker Compose](https://howtodoinjava.com/kafka/kafka-cluster-setup-using-docker-compose/)

## Versions

- Apache Kafka 4.3.1 (official image `apache/kafka:4.3.1`), KRaft mode, no ZooKeeper
- Docker Compose v2 (`docker compose`)
- Java client: JDK 25, kafka-clients 4.3.1, JUnit 6.1.3, Maven

## Files

| File | What it does |
|---|---|
| `docker-compose-single-node.yaml` | One node with the broker and controller roles, client port `localhost:9092` |
| `docker-compose.yaml` | Three combined broker+controller nodes, client ports `localhost:19092`, `19093`, `19094` |
| `docker-compose-add-broker.yaml` | A broker-only node `kafka-4` (`localhost:19095`) to add to the running 3-node cluster |
| `kafka-client/` | Maven project: a producer, a consumer and a JUnit test that connect to the 3-node cluster |

## Run the single node

```bash
docker compose -f docker-compose-single-node.yaml up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic deliveries --partitions 3
docker compose -f docker-compose-single-node.yaml down -v
```

## Run the 3-node cluster

```bash
docker compose up -d
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --topic deliveries --partitions 3 --replication-factor 3
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic deliveries
```

Stop one node to see leader election, then start it again:

```bash
docker compose stop kafka-1
docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-2:9092 --describe --topic deliveries
docker compose start kafka-1
```

## Run the Java client

The cluster from `docker-compose.yaml` and the `deliveries` topic must exist.

```bash
cd kafka-client
mvn -q compile exec:java    # sends parcel-4..6 and reads them back
mvn test                    # 3 tests, needs all 3 nodes up
```

Add a fourth, broker-only node:

```bash
docker compose -f docker-compose.yaml -f docker-compose-add-broker.yaml up -d
```

Stop and delete the containers and volumes:

```bash
docker compose -f docker-compose.yaml -f docker-compose-add-broker.yaml down -v
```

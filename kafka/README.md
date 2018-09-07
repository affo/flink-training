Before using Kafka's commands, [download
Kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz),
put the content of the installation into `kafka-install` folder, and `source
kafkarc`.

## Kafka CLI Cheatsheet

  * List topics

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --list
  ```

  * Describe a topic

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --describe --topic my-topic
  ```

  * Create a topic

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 3 --topic my-topic

  ```

  * Create a _compacted_ topic

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --create --topic my-compacted-topic --replication-factor 1 --partitions 3 --config cleanup.policy=compact
  ```

  * Add partitions

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --alter --topic my-topic --partitions 16
  ```

  * Delete a topic

  ```
  $ kafka-topics.sh --zookeeper localhost:2181 --delete --topic my-topic
  ```

  * Consume a topic to console

  ```
  $ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning
  ```

  * Consume a topic to console printing keys

  ```
  $ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning --property print.key=true
  ```

## Run Examples

To run a producer, for instance:

```
$ cd prodcons
$ mvn package
$ mvn exec:java -Dexec.mainClass=affetti.kafka.prodcons.SocketProducer -Dexec.args="...some args..."
```

The procedure is quite the same for other examples.

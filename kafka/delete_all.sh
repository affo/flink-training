#!/bin/bash
# This script deletes every topic in the kafka cluster.
# Please add Kafka's bin directory to PATH to make it work

topics=($(kafka-topics.sh --zookeeper localhost:2181 --list))
for topic in ${topics[*]}; do
  echo "Deleting $topic..."
  kafka-topics.sh --zookeeper localhost:2181 --delete --topic $topic
done

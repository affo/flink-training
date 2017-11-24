This folder contains scripts for:

  * Producing messages to Kafka on topic `test`, partition `0`;
  * Consuming messages from Kafka on the provided topic, partition `0`;

### Install

  ```
$ virtualenv venv
$ source rc
$ pip install -r requirements.txt
  ```

### Producing

  ```
$ python producer.py
# Kill with CTRL-C
  ```

### Consuming

  ```
$ python consumer.py <topic-name>
# Kill with CTRL-C
  ```


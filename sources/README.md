This folder contains scripts for:

  * Producing messages to Kafka on topic `test`, partition `0`;
  * Consuming messages from Kafka on the provided topic, partition `0`;

### Install

  ```
$ virtualenv venv
$ source rc
$ pip install -r requirements.txt
  ```

_NOTE_ if `pip install` goes wrong, try with this before

```
$ curl https://bootstrap.pypa.io/get-pip.py | python
```

### Producing

  ```
$ python producer.py
# Kill with CTRL-C
  ```

or simply run

```
$ ./produce.sh
```

This script will install the virtual environment if it does not exist (if some
error occurs, read the section above).

### Consuming

  ```
$ python consumer.py <topic-name>
# Kill with CTRL-C
  ```

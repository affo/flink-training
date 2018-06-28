This folder contains scripts for producing messages to Kafka.

### Install

  ```
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
  ```

_NOTE_ if `pip install` goes wrong, try with this before:

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

If something goes wrong check params in `sources.conf`.

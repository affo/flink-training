import time, random
from kafka import KafkaProducer
from config import CONF

host = CONF.get('DEFAULT', 'kafka_host')
port = CONF.get('DEFAULT', 'kafka_port')
broker = host + ':' + port
topic = CONF.get('DEFAULT', 'events_topic')

producer = KafkaProducer(bootstrap_servers=broker)

keys = ['foo', 'bar', 'buz']
i = 0
while True:
    msg = random.choice(keys)
    producer.send(topic, msg).get(timeout=60)
    i += 1
    if i % 1000 == 0:
        print '>>>', i, 'messages sent'

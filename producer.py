import json
import requests
from kafka import KafkaProducer

import config

response = requests.get(config.HTTP_STREAM_URL, stream=True)

if response.status_code != 200:
    print("Error occured, response returned non OK")
    exit(1)

kafka_producer = KafkaProducer(bootstrap_servers=config.BOOTSTRAP_SERVERS,
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in response.iter_lines():
    kafka_producer.send("raw-meetups", value=json.loads(i))

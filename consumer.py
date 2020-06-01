import json
import requests
from kafka import KafkaConsumer, TopicPartition

import config

response = requests.get(config.HTTP_STREAM_URL, stream=True)

if response.status_code != 200:
    print("Error occured, response returned non OK")
    exit(1)


def get_all_messages_from_topic(topic):
    users_partition = TopicPartition(topic=topic, partition=0)
    kafka_consumer = KafkaConsumer(topic, bootstrap_servers=config.BOOTSTRAP_SERVERS,
                                   value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                   auto_offset_reset="earliest",
                                   enable_auto_commit=False)
    result = []

    end_offset = kafka_consumer.end_offsets([users_partition])[users_partition]
    for i in kafka_consumer:
        result.append(i.value)
        if i.offset == end_offset - 1:
            break
    return result


# print(get_all_messages_from_topic("US-cities-every-minute2"))
# print(get_all_messages_from_topic("Programming-meetups"))
# print(len(get_all_messages_from_topic("US-meetups")))

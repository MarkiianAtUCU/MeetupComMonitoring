import json
from kafka import KafkaConsumer, TopicPartition
import config
from utils.S3Adapter import S3Adapter


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


s3Adapter = S3Adapter(config.AWS_CREDENTIALS, config.S3_OUTPUT_BUCKET)

for current_topic in config.TOPIC_LIST:
    s3Adapter.upload_file(f"meetup_com/{current_topic}.json", get_all_messages_from_topic(current_topic))

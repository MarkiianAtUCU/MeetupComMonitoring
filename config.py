HTTP_STREAM_URL = "http://stream.meetup.com/2/rsvps"

AWS_CREDENTIALS = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
}


SPARK_MASTER_URI = "spark:/<ip>:7077"
SPARK_CLUSTER = [
    ('spark.executor.memory', '1500m'),
    ('spark.executor.cores', '1'),
]

BOOTSTRAP_SERVERS = ['<ip1>:9092', '<ip2>:9092', '<ip3>:9092']
LOG_PREFIX = "log_meetup_reporter-"

TOPIC_LIST = ["US-cities-every-minute", "Programming-meetups", "US-meetups"]
S3_OUTPUT_BUCKET = "mmarkiian.s3"


import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col

import config


def prepare(spark):
    df = spark. \
        readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", ",".join(config.BOOTSTRAP_SERVERS)). \
        option("subscribe", "raw-meetups"). \
        option("startingOffsets", "earliest"). \
        load()

    json_struct = T.StructType([
        T.StructField('venue', T.StructType([
            T.StructField("venue_name", T.StringType()),
            T.StructField("lon", T.FloatType()),
            T.StructField("lat", T.FloatType()),
            T.StructField("venue_id", T.IntegerType())
        ])),
        T.StructField("visibility", T.StringType()),
        T.StructField("response", T.StringType()),
        T.StructField("guests", T.IntegerType()),
        T.StructField('member', T.StructType([
            T.StructField("member_id", T.IntegerType()),
            T.StructField("photo", T.StringType()),
            T.StructField("member_name", T.StringType())
        ])),
        T.StructField("rsvp_id", T.IntegerType()),
        T.StructField("mtime", T.LongType()),
        T.StructField('event', T.StructType([
            T.StructField("event_name", T.StringType()),
            T.StructField("event_id", T.StringType()),
            T.StructField("time", T.LongType()),
            T.StructField("event_url", T.StringType())
        ])),
        T.StructField('group', T.StructType([
            T.StructField("group_topics", T.ArrayType(T.StructType([
                T.StructField("urlkey", T.StringType()),
                T.StructField("topic_name", T.StringType())
            ]))),
            T.StructField("group_city", T.StringType()),
            T.StructField("group_country", T.StringType()),
            T.StructField("group_id", T.IntegerType()),
            T.StructField("group_name", T.StringType()),
            T.StructField("group_lon", T.FloatType()),
            T.StructField("group_urlname", T.StringType()),
            T.StructField("group_state", T.StringType()),
            T.StructField("group_lat", T.FloatType())
        ]))
    ])

    states_names = spark.read.json("data/USstate.json")

    json_parsed_df = df.select(
        col('timestamp'),
        F.from_json(col("value").cast("string"), json_struct).alias("json_parsed")
    ).filter(col('json_parsed').group.group_country == 'us').select(
        col('timestamp'),
        col('json_parsed.event.event_name'),
        col('json_parsed.event.event_id'),
        F.from_unixtime(col('json_parsed.event.time') / 1000).alias('time'),
        'json_parsed.group.group_topics.topic_name',
        'json_parsed.group.group_city',
        'json_parsed.group.group_country',
        'json_parsed.group.group_id',
        'json_parsed.group.group_name',
        'json_parsed.group.group_state'
    ).join(states_names, col("group_state") == states_names.code)

    return json_parsed_df

import pyspark.sql.functions as F
from pyspark.sql.functions import struct, window

import config


def task_2(json_parsed_df):
    result = json_parsed_df.withWatermark("timestamp", "1 minute").groupBy(
        window("timestamp", "1 minute", "1 minute")
    ).agg(
        struct(
            F.month('window.end').alias('month'),
            F.dayofmonth('window.end').alias('day_of_the_month'),
            F.hour('window.end').alias('hour'),
            F.minute('window.end').alias("minute"),
            F.collect_list('group_city').alias('cities')
        ).alias('res')
    ).select(F.to_json('res').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(config.BOOTSTRAP_SERVERS)) \
        .option("topic", "US-cities-every-minute") \
        .option("checkpointLocation", f"{config.LOG_PREFIX}topic_2")

    return result

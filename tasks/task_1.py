import pyspark.sql.functions as F
from pyspark.sql.functions import col, struct

import config


def task_1(json_parsed_df):
    result = json_parsed_df.select(
        struct(
            struct(
                col('event_name'),
                col('event_id'),
                col('time'),
            ).alias('event'),
            col('group_city'),
            col('group_country'),
            col('group_id'),
            col('group_name'),
            col('name').alias("group_state")
        ).alias('res')
    ).select(F.to_json('res').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(config.BOOTSTRAP_SERVERS)) \
        .option("topic", "US-meetups") \
        .option("checkpointLocation", f"{config.LOG_PREFIX}topic_1")

    return result

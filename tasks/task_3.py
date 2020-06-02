import pyspark.sql.functions as F
from pyspark.sql.functions import col, struct, lit, array, window

import config


def task_3(json_parsed_df):
    interesting_topics = array([
        lit('Computer programming'),
        lit('Big Data'),
        lit('Machine Learning'),
        lit('Python'),
        lit('Java'),
        lit('Web Development')
    ])

    result = json_parsed_df.select(
        struct(
            struct(
                col('event_name'),
                col('event_id'),
                col('time'),
            ).alias('event'),
            col("topic_name").alias("group_topics"),
            col('group_city'),
            col('group_country'),
            col('group_id'),
            col('group_name'),
            col('name').alias("group_state")
        ).alias('res')
    ).filter(F.arrays_overlap('res.group_topics', interesting_topics)).select(
        F.to_json('res').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(config.BOOTSTRAP_SERVERS)) \
        .option("topic", "Programming-meetups") \
        .option("checkpointLocation", f"{config.LOG_PREFIX}topic_3")

    return result

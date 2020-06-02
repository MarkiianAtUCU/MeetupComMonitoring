import findspark
import os

import config

findspark.init()
from pyspark.sql import SparkSession
import pyspark

from tasks.preparation import prepare
from tasks.task_1 import task_1
from tasks.task_2 import task_2
from tasks.task_3 import task_3

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'

conf = pyspark.SparkConf().setAll(config.SPARK_CLUSTER)
spark = SparkSession.builder.config(conf=conf).master(config.SPARK_MASTER_URI).getOrCreate()

prepared_df = prepare(spark)
task_1(prepared_df).start()
print("Started task 1")

task_2(prepared_df).start()
print("Started task 2")

last_worker = task_3(prepared_df).start()
print("Started task 3")

print("Waiting for termination")
last_worker.awaitTermination()

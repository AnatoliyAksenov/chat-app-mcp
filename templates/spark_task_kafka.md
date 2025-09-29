### Apache Spark application

Apache Spark ETL application for batch Kafka topic processing

filename: /dags/raw/orders/spark_task.py
```py
import os
import sys
import time

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as T

def task():

    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
    topic = os.environ.get('KAFKA_TOPIC')

    spark = (SparkSession.builder 
      .appName("ariflow_task_execute") 
      .enableHiveSupport() 
      .getOrCreate()
    )

    execution_date = os.environ.get('EXECUTION_DATE')


    if not execution_date:
        raise ValueError("Execution date is not provided!")

    if not topic:
        raise ValueError("Topic name is not provided")

    # Batch reading Kafka topic
    df = (spark.read 
    .format("kafka") 
    .option("kafka.bootstrap.servers", bootstrap_servers) 
    .option("subscribe", topic) 
    .option("startingOffsets", "earliest")  
    .option("endingOffsets", "latest")  
    .option('groupIdPrefix', 'spark_app')
    .load()
    )

    (df
     .withColumn('data', col('value').cast(T.StringType()))
     .withColumn("load_date", F.lit(execution_date))
     .select('topic', 'partition', 'offset', 'data', 'load_date')
     .write.insertInto('raw_messages.data') # Append
    )

    # very important block 
    # do not change this next three lines
    spark.catalog.clearCache()
    spark.stop()
    spark.sparkContext._gateway.jvm.System.exit(0)

if __name__ == '__main__':
    print('starting spark task')
    task()

    print('spark task done')
```
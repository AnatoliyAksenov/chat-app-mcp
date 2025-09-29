### Apache Spark application

Apache Spark ETL application

filename: /dags/raw/orders/spark_task.py
```py
import os
import sys
import time

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as T

def task():

    remote_endpoint = os.environ.get('S3_ENDPOINT')
    access_key_id = os.environ.get('S3_ACCESS_KEY')
    secret_access_key = os.environ.get('S3_SECRET_KEY')
    remote_bucket = os.environ.get('S3_REMOTE_BUCKET')

    spark = (SparkSession.builder 
      .appName("ariflow_task_execute") 
      .config(f"spark.hadoop.fs.s3a.bucket.{remote_bucket}.endpoint", remote_endpoint)
      .config(f"spark.hadoop.fs.s3a.bucket.{remote_bucket}.access.key", access_key_id)
      .config(f"spark.hadoop.fs.s3a.bucket.{remote_bucket}.secret.key", secret_access_key)
      .config(f"spark.hadoop.fs.s3a.bucket.{remote_bucket}.path.style.access", "true")
      .enableHiveSupport() 
      .getOrCreate()
    )

    execution_date = os.environ.get('EXECUTION_DATE')
    remote_path = os.environ.get('S3_PATH')

    if not execution_date:
        raise ValueError("Execution date is not provided!")

    if not remote_path.startswith('s3a://'):
        raise ValueError("Invalid S3 path format.")

    # read XML format
    df = (spark.read 
             .format("xml")
             .option("rowTag", "item") 
             .load(remote_path) 
         )

    # Flatten the nested structure
    flattened_df = df.select(
        # actual_unverified_data
        col("actual_unverified_data.purpose").alias("actual_unverified_purpose"),
        
        # dated_info
        col("dated_info.oti.letter").alias("oti_letter"),
        
        # metadata
        col("metadata.last_change_record_number").alias("metadata_last_change_record_number"),
        col("metadata.last_container_fixed_at").alias("metadata_last_container_fixed_at"),
        col("metadata.status").alias("metadata_status"),
        
        # object_common_data
        col("object_common_data.cad_number").alias("object_common_data.cad_number"),
        col("object_common_data.previously_posted").alias("object_common_data_previously_posted"),
        col("object_common_data.quarter_cad_number").alias("object_common_data_quarter_cad_number"),
        col("object_common_data.type.code").alias("object_common_data_type_code"),
        col("object_common_data.type.value").alias("object_common_data_type_value"),
        
        # object_formation
        col("object_formation.method").alias("formation_method"),
        
        # params
        col("params.area").cast("double").alias("params_area"),
        col("params.material.code").alias("material_code"),
        col("params.material.value").alias("material_value"),
        col("params.name").alias("building_name"),
        col("params.permitted_use").alias("params_permitted_use"),
        col("params.purpose.code").alias("purpose_code"),
        col("params.purpose.value").alias("purpose_value"),
        
        # record_info
        col("record_info.record_number").alias("record_info_record_number"),
        col("record_info.registration_date").alias("record_info_registration_date"),
        col("record_info.section_number").alias("record_info_section_number")
    )

    (flattened_df
     .withColumn("load_date", F.lit(execution_date))
     .write.insertInto('raw_orders.orders', mode='overwrite')
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
### Apache Spark application

Apache Spark ETL application

filename: /dags/raw/orders/spark_task.py
```py
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def task():
    spark = (SparkSession.builder
             .appName("orders_to_datalake_customers_daily_full")
             .enableHiveSupport()
             .getOrCreate())

    execution_date = os.environ.get('EXECUTION_DATE')
    if not execution_date:
        raise ValueError("EXECUTION_DATE environment variable is required.")

    conn_url = spark.conf.get("spark.conn_url")
    conn_user = spark.conf.get("spark.conn_user")
    conn_password = spark.conf.get("spark.conn_password")

    df = (spark.read.format("jdbc")
          .option("url", f"{conn_url}")
          .option("dbtable", "public.customers")
          .option("user", conn_user)
          .option("password", conn_password)
          .load())

    (df
     .withColumn("load_date", F.lit(execution_date))
     .write
     .mode("overwrite")
     .insertInto("raw_orders.customers"))

    spark.catalog.clearCache()
    spark.stop()
    spark.sparkContext._gateway.jvm.System.exit(0)


if __name__ == "__main__":
    print("Starting Spark ETL task for customers...")
    task()
    print("Spark ETL task completed.")
```
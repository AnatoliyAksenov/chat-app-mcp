### Apache Spark DQ application

Apache Spark DataQuolity Application

filename: /dags/raw/orders/spark_dq.py
```py
import os
import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def task():
    spark = (SparkSession.builder
             .appName("dq_customers")
             .enableHiveSupport()
             .getOrCreate())

    execution_date = os.environ.get('EXECUTION_DATE')
    endpoint = os.environ.get('ENDPOINT')

    if not execution_date or not endpoint:
        raise ValueError("EXECUTION_DATE and ENDPOINT are required.")

    df = spark.table("raw_orders.customers").filter(F.col("load_date") == execution_date)

    dq_metrics = df.select(
        F.count("*").alias("total_count"),
        F.countDistinct("company_name").alias("distinct_company_name"),
        F.countDistinct("city").alias("distinct_city"),
        F.count_if(F.col("created_by").isNull()).alias("null_created_by"),
        F.count_if(F.col("company_name").isNull()).alias("null_company_name"),
        F.count_if(F.col("email").isNull()).alias("null_email"),
        F.count_if(F.col("address").isNull()).alias("null_address"),
        F.count_if(F.col("city").isNull()).alias("null_city"),
    ).collect()[0]

    app = 'data_quality'

    _data = dq_metrics.asDict()
    data = "\n".join(f"{app}_{k} {v}" for k,v in _data.items()) + '\n'

    response = requests.post(
        endpoint,
        data=data,
    )

    if response.ok:
        print("Data quality metrics successfully reported.")
    else:
        print(f"Failed to report DQ: {response.status_code} {response.text}")
        raise RuntimeError("DQ reporting failed")

    spark.stop()
    spark.sparkContext._gateway.jvm.System.exit(0)

if __name__ == "__main__":
    print("Starting DQ validation...")
    task()
    print("DQ validation completed.")
```
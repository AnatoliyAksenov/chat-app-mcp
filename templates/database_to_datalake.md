### Database to Datalake AIrflow2 DAG

filename: /dags/raw/orders/orders_datalake_daily_dag.py
```py
'''
DAG: orders_to_datalake_customers_daily_full
Description: Daily full-load ETL of customer data from PostgreSQL to data lake
Schedule: 0 6 * * * (Midnight UTC daily)
Owner: team01@lct.dev.com
Tags: ['etl', 'postgresql', 'datalake', 'daily', 'full-load']
'''

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from helpers import generate_spark_submit_command, generate_dag_name, generate_spark_sql

# Define default arguments for the DAG
default_args = {
    'owner': 'team01@lct.dev.com',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

gitlab_token = Variable.get("gitlab_token")
spark_image = Variable.get("spark_image")
dq_endpoint = Variable.get("dq_endpoint")
docker_url = Variable.get('docker_url')

DAG_ID = generate_dag_name('orders_to_datalake_customers_daily_full', __file__)

# Define the DAG
dag = DAG(
    dag_id=DAG_ID,
    description='Daily full-load ETL of customer data from PostgreSQL to data lake',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'postgresql', 'datalake', 'daily', 'full-load'],
    default_view='graph',
    orientation='TB',
)

dag_path = Path(dag.fileloc).parent

# Task 1: Start
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Check Data Exists
check_data_exists_task = SqlSensor(
    task_id='check_data_exists_task',
    conn_id='jdbc_orders',
    sql='SELECT COUNT(*) FROM public.customers;',
    poke_interval=300,  # 5 minutes
    timeout=600,      # 10 minutes
    mode='reschedule',
    soft_fail=False,
    retries=2,
    execution_timeout=timedelta(seconds=600),
    dag=dag,
)

connection = BaseHook.get_connection('jdbc_orders')
username = connection.login
password = connection.password
hostname = connection.host
conf = {
    "conn_url": hostname,
    "conn_user": username,
    "conn_password": password
}

sql_cmd = generate_spark_sql(DAG_ID, sql_file=f'{dag_path}/sql/customers.sql', spark_configs=conf, gitlab_token=gitlab_token)

# SQL Task
sql_task = DockerOperator(
    task_id='sql_task',
    image=spark_image,
    docker_url=docker_url,
    network_mode='local',
    auto_remove=True,
    mount_tmp_dir=False,
    command=sql_cmd,
    execution_timeout=timedelta(seconds=3600),  
    dag=dag,
)


cmd = generate_spark_submit_command(DAG_ID, script_path=f"{dag_path}/tasks/spark_customers_etl.py", spark_configs=conf, gitlab_token=gitlab_token)

# ETL Task
etl_task = DockerOperator(
    task_id='etl_task',
    image=spark_image,
    docker_url=docker_url,
    network_mode='local',
    auto_remove=True,
    mount_tmp_dir=False,
    command=cmd,  # Deferred callable
    environment={
        'EXECUTION_DATE': '{{ ds }}'
    },
    execution_timeout=timedelta(seconds=3600),  # 60 minutes
    dag=dag,
)

dq_cmd = generate_spark_submit_command(DAG_ID, script_path=f"{dag_path}/tasks/dq_customers.py", spark_configs=conf, gitlab_token=gitlab_token)

# Data Quality Task
data_quality_task = DockerOperator(
    task_id='data_quality_task',
    image=spark_image,
    docker_url=docker_url,
    network_mode='local',
    auto_remove=True,
    mount_tmp_dir=False,
    command=dq_cmd,
    environment={
        'EXECUTION_DATE': '{{ ds }}',
        'ENDPOINT': dq_endpoint + '{{ dag.dag_id }}'
    },
    execution_timeout=timedelta(seconds=900), 
    dag=dag,
)

# Reporting (Success)
reporting_task = HttpOperator(
    task_id='reporting_task',
    http_conn_id='etl_reporting',
    endpoint='/metrics/job/etl_airflow/dag/{{ dag.dag_id }}/status/success',
    method='POST',
    headers={'Content-Type': 'text/plain'},
    data="""etl_airflow_fail 1
    """,
    trigger_rule='all_success',
    execution_timeout=timedelta(seconds=300),  # 5 minutes
    dag=dag,
)


# Reporting (Failure)
reporting_task_fail = HttpOperator(
    task_id='reporting_task_fail',
    http_conn_id='etl_reporting',
    endpoint='/metrics/job/etl_airflow/dag/{{ dag.dag_id }}/status/fail',
    method='POST',
    headers={'Content-Type': 'text/plain'},
    data="""etl_airflow_fail 1
    """,
    trigger_rule='one_failed',
    execution_timeout=timedelta(seconds=300),  # 5 minutes
    dag=dag,
)

# End
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start >> check_data_exists_task >> sql_task >> etl_task >> data_quality_task
data_quality_task >> reporting_task >> end
data_quality_task >> reporting_task_fail >> end

dag.doc_md = __doc__
```
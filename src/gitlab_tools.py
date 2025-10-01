import json

from fastmcp import FastMCP, Context

from pydantic import BaseModel, Field
from typing import Optional, Literal, List

from gitlab.v4.objects import ProjectManager

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from src.utils import get_gitlab_project, get_config, AppConfig


@dataclass
class AppContext:
    gitlab_project: ProjectManager
    config: AppConfig

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
        
        gitlab_project = get_gitlab_project()
        config = get_config()

        print("Gitlab project ready")
        yield AppContext(gitlab_project=gitlab_project, config=config)
        # clean
        pass
        
mcp = FastMCP()


@mcp.tool
async def gitlab_get_template_dag(etl_type: Literal['database_to_datalake', 's3_to_datalake', 'kafka_to_datalake']) -> str:
    """
    This tool provides template Airflow Dag for different etl processes
    """
    if etl_type == 'database_to_datalake':
        with open('templates/database_to_datalake.md', 'r') as f:
            dag_file = f.read()
    
        with open('templates/spark_task.md', 'r') as f:
            spark_task = f.read()
    
        with open('templates/spark_dq.md', 'r') as f:
            spark_dq = f.read()
    
        with open('templates/orders.md', 'r') as f:
            ddl = f.read()

        result = (
            f"Database to datalake template:"
            f"{dag_file}\n"
            "\n"
            f"{spark_task}\n"
            "\n"
            f"{spark_dq}\n"
            "\n"
            f"{ddl}\n"
            "**Additional Context:**\n"
            "- The ETL process uses DockerOperator to run Spark jobs in a containerized environment.\n"
            "- Spark application connects to PostgreSQL via JDBC using credentials stored in Airflow Connection `jdbc_<database name>`.\n"
            "- Data quality results are sent via HTTP POST to a monitoring endpoint.\n"
            "- Two reporting tasks handle success and failure scenarios using trigger rules.\n"
        )
        
        return result
    
    elif etl_type == 's3_to_datalake':
        with open('templates/s3_to_datalake.md', 'r') as f:
            dag_file = f.read()
    
        with open('templates/spark_task_s3.md', 'r') as f:
            spark_task = f.read()
    
        with open('templates/spark_dq.md', 'r') as f:
            spark_dq = f.read()
    
        with open('templates/orders.md', 'r') as f:
            ddl = f.read()

        result = (
            f"S3 to datalake template:"
            f"{dag_file}\n"
            "\n"
            f"{spark_task}\n"
            "\n"
            f"{spark_dq}\n"
            "\n"
            f"{ddl}\n"
            "**Additional Context:**\n"
            "- The ETL process uses DockerOperator to run Spark jobs in a containerized environment.\n"
            "- Spark application connects to S3 via S3AFileSystem implementation for spark. All jar dependencies already provided.\n"
            "- Credentials stored in the Airflow Connection `s3_<hostname>` are passed to the Spark application via environment variables.\n"
            "- Data quality results are sent via HTTP POST to a monitoring endpoint.\n"
            "- Two reporting tasks handle success and failure scenarios using trigger rules.\n"
        )        

        return result
    
    elif etl_type == 'kafka_to_datalake':
        with open('templates/s3_to_datalake.md', 'r') as f:
            dag_file = f.read()
    
        with open('templates/spark_task_kafka.md', 'r') as f:
            spark_task = f.read()
    
        with open('templates/spark_dq.md', 'r') as f:
            spark_dq = f.read()
    
        with open('templates/orders.md', 'r') as f:
            ddl = f.read()

        result = (
            f"Kafka to datalake template:"
            f"{dag_file}\n"
            "\n"
            f"{spark_task}\n"
            "\n"
            f"{spark_dq}\n"
            "\n"
            f"{ddl}\n"
            "**Additional Context:**\n"
            "- The ETL process uses DockerOperator to run Spark jobs in a containerized environment.\n"
            "- Spark application connects to Kafka via Spark-sql-kafka consumer. All jar dependencies already provided.\n"
            "- Credentials stored in the Airflow Connection `kafka_<hostname>` are passed to the Spark application via environment variables.\n"
            "- Data quality results are sent via HTTP POST to a monitoring endpoint.\n"
            "- Two reporting tasks handle success and failure scenarios using trigger rules.\n"
        )        

        return result
    else:
        return ''


class TaskInput(BaseModel):
    field: str = Field(..., description="Field name")
    value: str = Field(..., description="Field value")


@mcp.tool
async def create_task(
    task_title: str = Field(..., description="Task title"), 
    task_description: str = Field(..., description="Well formed task description with markdown."),
    form_data: List[TaskInput] = Field(..., description="Collected form data"), 
    ctx: Context = None
    ) -> str:
    """
    This tool submits a form the user have filled to create airflow pipeline (etl process). 
    """
    project: ProjectManager = ctx.request_context.lifespan_context.gitlab_project

    table_header = '\n| Parameter | Value |\n|:-----------------------| --------------------------------------------------:|\n'
    descr = task_description + '\n' +table_header + "\n".join(["| %s | %s |" % (item.field,item.value) for item in form_data])

    issue_data = {
        'title': task_title,
        'description': descr,
        'labels': ['database-to-datalake']
    }

    issue = project.issues.create(issue_data)
    
    url = f'https://gitlab.it-brew-lct2025.ru/data-engineering/airflow/-/work_items/{issue.id}'
    return f'Task id: {issue.id}, link: {url}'

import io
import os
import json
import httpx

from fnmatch import fnmatch
from chardet import detect as chardetect
from fastmcp import FastMCP, Context

from pydantic import BaseModel, Field
from typing import Optional, Literal, List


mcp = FastMCP()


@mcp.tool 
async def get_available_connections(ctx: Context) -> str:
    """
    This tool is for getting a list of available Airflow connections
    """
    app_conf = ctx.request_context.lifespan_context.config
    auth = httpx.BasicAuth(app_conf.AIRFLOW_USER, app_conf.AIRFLOW_PASSWORD)
    res = httpx.get(app_conf.AIRFLOW_URL+'/api/v1/connections', auth=auth)
    return res.text


@mcp.tool 
async def get_available_dags(ctx: Context) -> str:
    """
    This tool is for getting a list of available Airflow DAGs
    """
    app_conf = ctx.request_context.lifespan_context.config
    auth = httpx.BasicAuth(app_conf.AIRFLOW_USER, app_conf.AIRFLOW_PASSWORD)
    res = httpx.get(app_conf.AIRFLOW_URL+'/api/v1/dags', auth=auth)
    return res.text


@mcp.tool 
async def get_available_dag_tasks(dag_id:str = Field(..., description="Airflow DAG id"), ctx: Context = None) -> str:
    """
    This tool is for getting a list of tasks in required Airflow DAG
    """
    app_conf = ctx.request_context.lifespan_context.config
    auth = httpx.BasicAuth(app_conf.AIRFLOW_USER, app_conf.AIRFLOW_PASSWORD)
    res = httpx.get(app_conf.AIRFLOW_URL+f'/api/v1/dags/{dag_id}/tasks', auth=auth)
    return res.text


@mcp.tool 
async def get_available_variables(ctx: Context) -> str:
    """
    This tool is for getting list of available Airflow vairables
    """
    app_conf = ctx.request_context.lifespan_context.config
    auth = httpx.BasicAuth(app_conf.AIRFLOW_USER, app_conf.AIRFLOW_PASSWORD)
    res = httpx.get(app_conf.AIRFLOW_URL+'/api/v1/variables', auth=auth)
    return res.text


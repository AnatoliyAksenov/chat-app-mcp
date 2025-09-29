import asyncio
import logging

from fastmcp import FastMCP

from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import NoSuchTableError

from pydantic import BaseModel, Field
from typing import Optional, Literal

from src.db_utils import get_table_ddl, get_table_ddl_with_partitions, get_table_sample

LOGGER = logging.getLogger(__name__)


mcp = FastMCP()

@mcp.tool
async def test_db_connection(connection_url:str = Field(..., description="URL to database in sqlalchemy format") ) -> Literal["Success", "Error"]:
    """
    This tool is for trying to get access to the database and executes simple query: `SELECT 1 n` 
    """
    LOGGER.info('test db connection')
    engine = create_engine(url=connection_url)
    conn = engine.connect()

    cur = conn.execute(text('SELECT 1 n'))

    res = cur.fetchall()

    cols = list(cur.keys())

    return "Success" if all([cols[0] == 'n', res[0], res[0][0] == 1]) else "Error"


@mcp.tool
async def test_db_table_exists(
    connection_url: str = Field(..., description="URL to database in sqlalchemy format"), 
    table_name: str = Field(..., description="Table name to test in format schema.table_name, if schema is empty use default schema for specific engine")
    ) -> bool:
    """
    This tool is for trying to check the table exists in the provided database and executes the simple query: `SELECT (SELECT 1 n from table where 1=1 limit 1) n` 
    """
    LOGGER.info('test db table exists')
    engine = create_engine(url=connection_url)
    inspector = inspect(engine)

    _schema, _table_name = table_name.split('.') if table_name.count('.') == 1 else ['public', table_name]

    return inspector.has_table(_table_name, _schema)


@mcp.tool
async def get_db_table_ddl(
    connection_url: str = Field(..., description="URL to database in sqlalchemy format"), 
    table_name: str = Field(..., description="Table name to test in format schema.table_name, if schema is empty use default schema for specific engine")
    ) -> str:

    """
    This tool is for generating DDL for passed table
    """
    LOGGER.info('get table ddl')

    engine = create_engine(url=connection_url)
    _schema, _table_name = table_name.split('.') if table_name.count('.') == 1 else ['public', table_name]
    result = get_table_ddl_with_partitions(engine=engine, table_name=_table_name, schema=_schema)

    return result


@mcp.tool
async def get_db_table_sample(connection_url: str = Field(..., description="URL to database in sqlalchemy format"), 
    table_name: str = Field(..., description="Table name to test in format schema.table_name, if schema is empty use default schema for specific engine")
    ) -> list[dict]:
    """
    This tool is for extracting sample data from passed table
    """
    LOGGER.info('test db table exists')
    engine = create_engine(url=connection_url)

    _schema, _table_name = table_name.split('.') if table_name.count('.') == 1 else ['public', table_name]

    result = get_table_sample(engine=engine, table=_table_name, sample_size=100, schema=_schema)

    return result


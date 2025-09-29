import asyncio
import logging

from fastmcp import FastMCP

from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import NoSuchTableError

from pydantic import BaseModel, Field
from typing import Optional, Literal, List

from src.db_utils import get_table_ddl, get_table_ddl_with_partitions, get_table_sample

LOGGER = logging.getLogger(__name__)


mcp = FastMCP()

# Form base model
from typing import List, Optional
from pydantic import BaseModel, Field

class FormField(BaseModel):
    field: str = Field(..., description="Required field")
    description: str = Field(..., description="Description of required field")
    values: Optional[List[str]] = Field(None, description="Available values")
    example: Optional[str] = Field(None, description="Available examples or snippets")
    optional: Optional[bool] = Field(False, description="If this field optional")
    depends_on: Optional[List[str]] = Field(None, description="Names of dependent fields")

class Form(BaseModel):
    id: str = Field(..., description="Form id")
    description: str = Field(..., description="Form description")
    form_submit_tool_name: str = Field(..., description="Tool name to submit form data")
    form_fields: List[FormField] = Field(..., description="Form fields")

class DatabaseDatalake(BaseModel):
    id: str = Field("database_to_datalake", description="Form id")
    description: str = Field("Form for etl process from databases to datalake", description="Form description")
    form_submit_tool_name: str = Field("create_task", description="Tool name to submit form data")
    
    # Predefined form fields as a class attribute
    form_fields: List[FormField] = Field(
        default_factory=lambda: [
            FormField(
                field="DAG ID",
                description="The Airflow DAG ID. E.g. orders_customer_to_datalake_daily"
            ),
            FormField(
                field='source_database', 
                description="It should be fully completed url for sqlalchemy: dialect://user:password@host:port/database?additional+params"
            ),
            FormField(
                field="database_available", 
                description="Indicates if the source database is reachable. Check this by calling the appropriate connectivity tool."
            ),
            FormField(
                field="source_table", 
                description="The table name in schema.table_name format"
            ),
            FormField(
                field="source_table_ddl", 
                description="User have to provide this field if database do not available or you can't get it from tool", 
                depends_on=["database_available"]
            ),
            FormField(
                field="interval",
                description="The scheduling interval for the ETL process. Must be in `cron` or Airflow format."
            ),
            FormField(
                field="required_transformations", 
                description="Describes the required data transformations. Options: \n"
                            "- `store_as_is` (no transformation) content stores into table cell with additional attributes\n"
                            "- `flatten` (flatten structured data and generate a table)\n"
                            "- `cast types` if necessary type casting\n"
                            "- `filtering` filter income data if necessary",  
                optional=True
            ),
            FormField(
                field="target_table", 
                description="The name of the target table. Format: `raw_<source_database>.<source_table>`. "
                            "If provided, validate this format. Only the `raw_` prefix is permitted for raw data; "
            ),
            FormField(
                field="target_storing_type", 
                description="Defines the table update strategy. The system will attempt to determine the best type. \n"
                            "Default: measures -> `partition`, dimensions -> `full`. \n"
                            "Available strategies: \n"
                            "`overwrite` - replace the entire table on each run; \n"
                            "`append` - add new records without affecting existing ones; \n"
                            "`partition` - store changes in a new partition; \n"
                            "`full` - store a complete snapshot in a new partition; \n"
                            "`scd2` - use Slowly Changing Dimension type 2 to track historical changes.\n",
                values=["overwrite", "append", "partition", "full", "scd2"],
                optional=True
            ),
            FormField(
                field="capture_changes_column", 
                description="The column used to identify changed data (e.g., `ins_date` for measures, `last_update_date` for dimensions). "
                            "The system will attempt to auto-detect this column.",
                optional=True,
                depends_on=['target_storing_type']
            ),
            FormField(
                field="data_quality", 
                description="User-defined rules for calculating data quality metrics. "
                            "If not provided, default metrics (row count, null counts per column) are calculated.",
                optional=True
            )
        ],
        description="Predefined form fields for database-to-data-lake ETL process"
    )


class FileStorageDatalake(BaseModel):
    id: str = Field("filestorage_to_datalake", description="Form id")
    description: str = Field("Form for etl process from file storage to datalake", description="Form description")
    form_submit_tool_name: str = Field("filestorage_to_datalake_create_task", description="Tool name to submit form data")
    
    # Predefined form fields as a class attribute
    form_fields: List[FormField] = Field(
        default_factory=lambda: [
            FormField(
                field="DAG ID",
                description="The Airflow DAG ID. E.g. orders_customer_to_datalake_daily"
            ),
            FormField(
                field='source_url', 
                description="The complete URL for the source file system. Supported protocols: `s3`, `smb`, `ftp`, `ftps`."
            ),
            FormField(
                field='source_credentials', 
                description="All necessary credentials and connection parameters for the source file system."
            ),
            FormField(
                field="source_available", 
                description="Indicates if the file source is reachable. Check this by calling the appropriate connectivity tool."
            ),
            FormField(
                field="source_file", 
                description="The name of the file or folder. To match multiple files, use a wildcard pattern (e.g., `/path/to/folder/file*.csv`)."
            ),
            FormField(
                field="source_schema", 
                description="Schema definition (JSON, XSD, or XML) for the source files.", 
                depends_on=["database_available"]
            ),
            FormField(
                field="interval",
                description="The scheduling interval for the ETL process. Must be in `cron` or Airflow format."
            ),
            FormField(
                field="required_transformations", 
                description="Describes the required data transformations. Options: \n"
                            "- `store_as_is` (no transformation) content stores into table cell with additional attributes\n"
                            "- `flatten` (flatten structured data and generate a table)\n"
                            "- `cast types` if necessary type casting\n"
                            "- `filtering` filter income data if necessary", 
                optional=True
            ),
            FormField(
                field="target_table", 
                description="The name of the target table. Format: `raw_<source_system>.<source_table>`. "
                            "If provided, validate this format. Only the `raw_` prefix is permitted for raw data; "
                            "use `stage_` for files stored without transformation."
            ),
            FormField(
                field="target_storing_type", 
                description="Defines the table update strategy. The system will attempt to determine the best type. \n"
                       "Default: measures -> `partition`, dimensions -> `full`. \n"
                       "Available strategies: \n"
                       "`overwrite` - replace the entire table on each run; \n"
                       "`append` - add new records without affecting existing ones; \n"
                       "`partition` - store changes in a new partition; \n"
                       "`full` - store a complete snapshot in a new partition; \n"
                       "`scd2` - use Slowly Changing Dimension type 2 to track historical changes.\n",
                values=["overwrite", "append", "partition", "full", "scd2"],
                optional=True
            ),
            FormField(
                field="capture_changes_column", 
                description="The field used to identify changed data (e.g., `ins_date` for measures, `last_update_date` for dimensions). "
                            "The system will attempt to auto-detect this column.",
                optional=True,
                depends_on=['target_storing_type']
            ),
            FormField(
                field="data_quality", 
                description="User-defined rules for calculating data quality metrics. "
                            "If not provided, default metrics (row count, null counts per column) are calculated.",
                optional=True
            )
        ],
        description="Predefined form fields for configuring a file-to-data-lake ETL process."
    )


class InternetSourceDatalake(BaseModel):
    id: str = Field("internetsource_to_datalake", description="Form ID for internet source ingestion.")
    description: str = Field("Form for ETL process from public internet APIs or URLs to datalake", description="Form description")
    form_submit_tool_name: str = Field("internetsource_to_datalake_create_task", description="Tool name to submit form data")
    
    # Predefined form fields as a class attribute
    form_fields: List[FormField] = Field(
        default_factory=lambda: [
            FormField(
                field="DAG ID",
                description="The Airflow DAG ID. E.g. orders_customer_to_datalake_daily"
            ),
            FormField(
                field='source_url', 
                description="The full URL (HTTP/HTTPS) to the data source. Supported formats: `JSON`, `XML`, `CSV` (with header)."
            ),
            FormField(
                field='source_credentials', 
                description="Credentials if the endpoint requires authentication (e.g., API Key, Bearer Token, Basic Auth). "
                           "Provide as a key-value JSON object or a connection URI.",
                optional=True
            ),
            FormField(
                field="request_method",
                description="The HTTP method to use for the request. Default is `GET`.",
                values=["GET", "POST"],
                default="GET",
                optional=True
            ),
            FormField(
                field="request_body",
                description="A JSON payload to send in the request body. Required for `POST` requests or certain API endpoints.",
                optional=True,
                depends_on=["request_method"]
            ),
            FormField(
                field="source_available", 
                description="Indicates if the HTTP endpoint is reachable and returns a successful status code (e.g., 200)."
            ),
            FormField(
                field="response_type", 
                description="The expected data format returned by the endpoint. The system will attempt to auto-detect this.",
                values=["json", "xml", "csv"],
                optional=True
            ),
            FormField(
                field="json_root_path", 
                description="For JSON responses: the JSONPath to the array of records. "
                           "Example: `$.data.results` or `$` for the root array. Required for nested JSON.",
                optional=True,
                depends_on=["response_type"]
            ),
            FormField(
                field="pagination_type", 
                description="Defines how the API handles pagination. Required to fetch all available data.",
                values=["none", "page_number", "offset", "cursor", "next_url"],
                default="none",
                optional=True
            ),
            FormField(
                field="interval",
                description="The scheduling interval for the ETL process. Must be in `cron` or Airflow format."
            ),
            FormField(
                field="required_transformations", 
                description="Describes the required data transformations. Options: \n"
                            "- `normalize_json` (flatten a nested JSON object into a table) \n"
                            "- `parse_xml` (convert XML into a relational table) \n"
                            "- `cast_types` (apply necessary type casting for CSV/JSON data) \n"
                            "- `filtering` (filter incoming data based on conditions)", 
                optional=True
            ),
            FormField(
                field="target_table", 
                description="The name of the target table. Format: `raw_<source_system>.<entity_name>`. "
                           "If provided, validate this format. The `raw_` prefix is required."
            ),
            FormField(
                field="target_storing_type", 
                description="Defines the table update strategy. \n"
                           "Default for API data: `append` (for event streams) or `full` (for snapshots). \n"
                           "Available strategies: \n"
                           "`overwrite` - replace the entire table on each run; \n"
                           "`append` - add new records without affecting existing ones; \n"
                           "`partition` - store API responses in daily partitions; \n"
                           "`full` - store a complete snapshot in a new partition; \n"
                           "`scd2` - use Slowly Changing Dimension type 2 to track historical changes.",
                values=["overwrite", "append", "partition", "full", "scd2"],
                optional=True
            ),
            FormField(
                field="capture_changes_column", 
                description="The column used to identify changed data. For APIs, this is often a field like `last_updated` or `created_at`. "
                           "The system will attempt to auto-detect this field.",
                optional=True,
                depends_on=['target_storing_type']
            ),
            FormField(
                field="data_quality", 
                description="User-defined rules for calculating data quality metrics for the API data. "
                           "If not provided, default metrics (row count, null counts per column, unique keys) are calculated.",
                optional=True
            )
        ],
        description="Predefined form fields for configuring an internet API-to-data-lake ETL process."
    )


@mcp.tool
async def get_form_properties_etl_database_to_datalake() -> DatabaseDatalake:
    """
    Retrieves the configuration form for creating an ETL pipeline from databases to data lake.
    
    This is the first step in a two-step process to create an Airflow ETL pipeline that extracts data 
    from various database sources (PostgreSQL, MySQL, MariaDB, ClickHouse) and loads it into 
    the data lake storage system.
    
    CRITICAL WORKFLOW INSTRUCTIONS:
    1. Call this tool first to obtain the required configuration fields
    2. Collect all necessary connection details, authentication credentials, transformation rules and etc
    3. Give users permission to create task
    4. Call `create_task` to initialise the pipeline
    """
    instance = DatabaseDatalake()
    
    return instance


@mcp.tool
async def get_form_properties_etl_filestorage_to_datalake() -> DatabaseDatalake:
    """
    Retrieves the configuration form for creating an ETL pipeline from file storage systems to data lake.
    
    This is the first step in a two-step process to create an Airflow ETL pipeline that extracts data 
    from various file storage sources (AWS S3, SMB shares, FTP, SFTP) 
    and loads it into the data lake storage system.
    
    CRITICAL WORKFLOW INSTRUCTIONS:
    1. Call this tool first to obtain the required configuration fields
    2. Collect all necessary connection details, authentication credentials, transformation rules and etc
    3. Give users permission to create task
    4. Call `create_task` to initialise the pipeline
    """
    instance = FileStorageDatalake()
    
    return instance


@mcp.tool
async def get_form_properties_etl_internetsource_to_datalake() -> DatabaseDatalake:
    """
    Retrieves the configuration form for creating an ETL pipeline from internet sources to data lake.
    
    This is the first step in a two-step process to create an Airflow ETL pipeline that extracts data 
    from various internet sources (REST APIs, RSS feeds) and loads it into the data lake storage system.
    
    CRITICAL WORKFLOW INSTRUCTIONS:
    1. Call this tool first to obtain the required configuration fields
    2. Collect all necessary connection details, authentication credentials, transformation rules and etc
    3. Give users permission to create task
    4. Call `create_task` to initialise the pipeline
    """
    instance = InternetSourceDatalake()
    
    return instance


@mcp.tool
async def get_internal_resources() -> List[str]:
    """
    Returns a curated list of all available internal development and data infrastructure services.

    Use this tool when the user asks about:
    - Available tools for ETL, data storage, or workflow orchestration
    - URLs or credentials for platform services (e.g., GitLab, Airflow, MinIO)
    - Internal endpoints like Kafka, PostgreSQL, or JupyterLab
    - How to access development environments or monitoring dashboards

    The response includes service names, access URLs or endpoints, brief descriptions, 
    and authentication details (where safe to share). This helps users quickly locate 
    and connect to the resources they need without manual lookup.
    
    Returns:
        List[str]: Human-readable entries in the format:
            "<URL or endpoint> - <Service name>: <Description>. Credentials: <creds if applicable>"
    """
    user_resources = [
        "https://s3.it-brew-lct2025.ru/ - MinIO Object Storage: Web UI for managing and uploading large files. Credentials: devuser/devpassword",
        "https://gitlab.it-brew-lct2025.ru/ - GitLab: Version control with CI/CD pipelines. Credentials: mentor/Lct2025",
        "https://airflow.it-brew-lct2025.ru/ - Apache Airflow: Workflow orchestration for ETL jobs. Credentials: mentor/Lct2025",
        "https://grafana.it-brew-lct2025.ru/ - Grafana: Monitoring and observability dashboards. Credentials: mentor/Lct2025",
        "https://workspace.it-brew-lct2025.ru/lab? - JupyterLab: Interactive notebook environment. Token: Hfrtnf99",
        "https://langfuse.it-brew-lct2025.ru/ - Langfuse: LLM tracing and agent usage analytics. Credentials: mentor@it-brew-lct2025.ru / Hfrtnf99!",
        "postgresql://10.0.0.7:5432/orders - PostgreSQL Database: Test database for ETL development. Credentials: devuser/devpassword",
        "10.0.0.200:9001 - Internal S3-compatible storage (MinIO): For inter-service file exchange. Pre-configured buckets: project01–03. Users may create more.",
        "10.0.0.10:9092 - Apache Kafka: Streaming platform for real-time data pipelines",
        "https://1c.it-brew-lct2025.ru/test1/ru-RU/ - 1C ERP Demo: Illustrates Kafka integration with enterprise systems",
    ]
    return user_resources
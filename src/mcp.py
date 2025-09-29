from fastmcp import FastMCP

from contextlib import asynccontextmanager, AsyncExitStack
from collections.abc import AsyncIterator

from src.db_tools import mcp as db_tools
from src.form_tools import mcp as form_tools
from src.s3_tools import mcp as s3_tools
from src.smb_tools import mcp as smb_tools
from src.web_tools import mcp as web_tools
from src.kafka_tools import mcp as kafka_tools
from src.airflow_tools import mcp as airflow_tools

from src.gitlab_tools import mcp as gitlab_tools, lifespan as gitlab_lifespan, AppContext

@asynccontextmanager
async def combined_lifespan(server: FastMCP) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        # Enter each sub-lifespan and attach results to server
        git_ctx: AppContext = await stack.enter_async_context(gitlab_lifespan(server))
        #server.gitlab_project = git_ctx.gitlab_project


        print("All lifespans initialized.")
        yield  git_ctx 

mcp = FastMCP(lifespan=combined_lifespan)

mcp.mount(db_tools)
mcp.mount(form_tools)
mcp.mount(s3_tools)
mcp.mount(smb_tools)
mcp.mount(gitlab_tools)
mcp.mount(web_tools)
mcp.mount(kafka_tools)
mcp.mount(airflow_tools)
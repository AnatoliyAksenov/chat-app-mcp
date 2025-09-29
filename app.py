import logging

from fastapi import FastAPI

from src.mcp import mcp

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
logger = logging.getLogger(__name__)

mcp_app = mcp.http_app(path="/", transport="streamable-http")
sse_app = mcp.sse_app(path='/')

#routes = [*mcp_app.routes, *sse_app.routes]

app = FastAPI(
    title="MCP server for DE agent",
    lifespan=mcp_app.lifespan
)

app.mount("/mcp", mcp_app, "MCP Server")
app.mount("/sse", sse_app, "SSE implementation")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('app:app', host="127.0.0.1", port=8001, forwarded_allow_ips="*", use_colors=False)
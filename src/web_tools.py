import io
import os
import json
import zlib
import httpx
import base64

from fnmatch import fnmatch
from chardet import detect as chardetect
from fastmcp import FastMCP

from pydantic import BaseModel, Field
from typing import Optional, Literal


mcp = FastMCP()

@mcp.tool 
async def test_link(
    url: str = Field(..., description="The complete URL to fetch data from. Must include protocol (http:// or https://)"),
    ) -> str:
    """
    This tool is for checking if an URL is available
    """
    proxy = os.environ.get('USE_PROXY')
    try:
        if proxy:
            res = httpx.get(url, proxy=proxy)
        else:
            res = httpx.get(url)
        return f'Success. Code: {res.status_code}'
    
    except httpx.HTTPStatusError as e:
        return f"HTTP Error: {e.response.status_code} - {e.response.text}"
    except httpx.RequestError as e:
        return f"Request Error: {e}"
    except Exception as e:
        return f"Unexpected Error: {e}"
    

@mcp.tool 
async def get_link_sample(
    url: str = Field(..., description="The complete URL to fetch data from. Must include protocol (http:// or https://)"),
    method: Literal['GET', 'POST'] = Field('GET', description="HTTP method for the request. Default is `GET`"),
    body: Optional[str] = Field(None, description="JSON string to send in request body for POST requests. Required for POST methods or APIs expecting JSON payload")
) -> str:
    """
    Fetches data from a web URL and returns a textual sample. 
    Supports both GET and POST requests with optional JSON payload.
    Useful for testing API endpoints, checking website availability, or sampling web content.
    """
    proxy = os.environ.get('USE_PROXY')
    
    request_kwargs = {}
    
    if method == 'POST' and body:
        try:
            json_data = json.loads(body)
            request_kwargs['json'] = json_data
        except json.JSONDecodeError:
            return f"Error: Invalid JSON format in body parameter"
    
    if proxy:
        request_kwargs['proxy'] = proxy
    
    try:
        # Pydantic schema validation have to check incoming literals.
        # This chek is for internal tool calls.
        if method == 'GET':
            response = httpx.get(url, **request_kwargs)
        elif method == 'POST':
            response = httpx.post(url, **request_kwargs)
        else:
            return f"Error: Unsupported HTTP method '{method}'"
        
        response.raise_for_status()
        
        content_sample = response.text[:2000] 
        return f'Success. Status Code: {response.status_code}\nSample Content: <sample>{content_sample}</sample>'
        
    except httpx.HTTPStatusError as e:
        return f"HTTP Error: {e.response.status_code} - {e.response.text}"
    except httpx.RequestError as e:
        return f"Request Error: {e}"
    except Exception as e:
        return f"Unexpected Error: {e}"
    

@mcp.tool 
async def transform_ditaa_to_markdown_image(
    ditaa_scheme:str = Field(..., description="Ditaa scheme")
) -> str:
    """
    This tool is for transform Ditaa scheme to markdown image.

    Example of Ditaa scheme with all supported features:
    +-----------------------------+
    | +--------+     +-----------+|     +-----------+     /----------+
    | |        |     |           ||     |  {s}      |     |          |
    | | start  | --> | process 1 || --> | process 2 | --> | reporting|
    | |        |     |           ||     |           |     |          |
    | +--------+     +-----------+|     +-----------+     +----------+
    |                             |                            |
    +-----------------------------+                            |
                      |  |                                     v
                      |  |                   +----------------------+
                      |  |                   |  cPNK                |
                      |  |                   |  split into two +----+
                      |  \-------push------->|  parallel paths |cBLU|
                      |                      +-----------------+----+
                      |                             |             |
                      |                             |             |
                      |                             v             v
                      \------------------\    +----------+    /----------+
                                         |    |          |    |          |
                                         |    | to email |    | to slack |
                                         \--->|          |    |          |
                                              +----------+    +----------/
                                                    |             |
                                                    |             |
                                                    v             v
                                               +-----------------------+
                                               |                       |
                                               |        end            |
                                               |                       |
                                               +-----------------------+

    Rules:
      cBLU - fills the box with blue color  
      cPNK - fills the box with pink color  
      {s}  - makes the box look like a database barrel  
      \    - applies curved corners
    Returns:
      ![schema](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==)

    """
    q = base64.urlsafe_b64encode(zlib.compress(ditaa_scheme.encode('utf-8'), 9)).decode('ascii')
    proxy = os.environ.get('USE_PROXY')

    request_kwargs = {}
    if proxy:
        request_kwargs['proxy'] = proxy

    resp = httpx.get(f'https://kroki.io/ditaa/png/{q}', **request_kwargs)
    t = resp.content
    return f'![schema](data:image/png;base64,{base64.b64encode(t).decode("ascii")})'

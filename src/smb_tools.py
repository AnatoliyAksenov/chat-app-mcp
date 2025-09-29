import io

from smb.SMBConnection import SMBConnection
from smb.base import SharedFile

from fnmatch import fnmatch
from chardet import detect as chardetect
from fastmcp import FastMCP

from pydantic import BaseModel, Field
from typing import Optional, Literal


mcp = FastMCP()

@mcp.tool 
def test_smb_connection(
    username: str = Field(..., description="Smb server username"),
    password: str = Field(..., description="Smb server user password"),
    server_ip: str = Field(..., description="Smb server ip"),
    server_port: int = Field(445, description="Smb server port"),
    my_name: str = Field("", description="Client dns name"),
    remote_name: str = Field("", description="DNS name of the server"),
    ) -> str:
    """
    This tool is for checking if the smb server available
    """
    try:
        conn = SMBConnection(username, password, my_name=my_name, remote_name=remote_name, use_ntlm_v2=True)
        conn.connect(server_ip, server_port)
        
        shares = conn.listShares()
        res = "\n".join([share.name for share in shares])
        conn.close()

        return 'Success'
    except Exception as e:
        return f"Error: {e}"
    

@mcp.tool 
def test_smb_share_exists(
    username: str = Field(..., description="Smb server username"),
    password: str = Field(..., description="Smb server user password"),
    server_ip: str = Field(..., description="Smb server ip"),
    share_name: str = Field(..., description="Share name"),
    server_port: int = Field(445, description="Smb server port"),
    my_name: str = Field("", description="Client dns name"),
    remote_name: str = Field("", description="DNS name of the server"),
    ) -> str:
    """
    This tool is for checking if share exists in the smb server
    """
    try:
        conn = SMBConnection(username, password, my_name=my_name, remote_name=remote_name, use_ntlm_v2=True)
        conn.connect(server_ip, server_port)
        
        shares = conn.listShares()
        for share in shares:
            if share.name == share_name:
                conn.close()
                return 'Success'

        conn.close()
        return 'Share not found'
    except Exception as e:
        return f"Error: {e}"
    

@mcp.tool 
def test_smb_file_exists(
    username: str = Field(..., description="Smb server username"),
    password: str = Field(..., description="Smb server user password"),
    server_ip: str = Field(..., description="Smb server ip"),
    share_name: str = Field(..., description="Share name"),
    path: str = Field(..., description="Path in shared folder"),
    file_mask: str = Field("*", description="File filter in glob format, eg `*.csv`, `messages_*.json`"),
    server_port: int = Field(445, description="Smb server port"),
    my_name: str = Field("", description="Client dns name"),
    remote_name: str = Field("", description="DNS name of the server"),
    ) -> str:
    """
    This tool is for checking if the files by mask exists in share and path in the smb server
    """
    try:
        conn = SMBConnection(username, password, my_name=my_name, remote_name=remote_name, use_ntlm_v2=True)
        conn.connect(server_ip, server_port)
        
        file_list = conn.listPath(share_name, path)
        print(file_list)

        content = []
        for f in file_list:
            if isinstance(f, SharedFile) and fnmatch(f.filename, file_mask):
                content.append(f"{f.filename}\t{f.file_size}")

        if content:
            conn.close()
            return "\n".join(content)
    
        conn.close()
        return 'Share or files not found'
    except Exception as e:
        return f"Error: {e}"


@mcp.tool 
def get_smb_file_list(
    username: str = Field(..., description="Smb server username"),
    password: str = Field(..., description="Smb server user password"),
    server_ip: str = Field(..., description="Smb server ip"),
    share_name: str = Field(..., description="Share name"),
    path: str = Field(..., description="Path in shared folder"),
    file_mask: str = Field("*", description="File filter in glob format, eg `*.csv`, `messages_*.json`"),
    server_port: int = Field(445, description="Smb server port"),
    my_name: str = Field("", description="Client dns name"),
    remote_name: str = Field("", description="DNS name of the server"),
    ) -> str:
    """
    This tool is for checking if the files or path exists in share in the smb server
    """
    try:
        conn = SMBConnection(username, password, my_name=my_name, remote_name=remote_name, use_ntlm_v2=True)
        conn.connect(server_ip, server_port)
        
        file_list = conn.listPath(share_name, path)

        content = []
        for f in file_list:
            if isinstance(f, SharedFile) and fnmatch(f.filename, file_mask):
                content.append(f"{f.filename}\t{f.file_size}")

        if content:
            conn.close()
            return "\n".join(content)
    
        conn.close()
        return 'Share or files not found'
    except Exception as e:
        return f"Error: {e}"
    

@mcp.tool 
def get_smb_file_sample(
    username: str = Field(..., description="Smb server username"),
    password: str = Field(..., description="Smb server user password"),
    server_ip: str = Field(..., description="Smb server ip"),
    share_name: str = Field(..., description="Share name"),
    path: str = Field(..., description="Path in shared folder"),
    file_mask: str = Field("*", description="File filter in glob format, eg `*.csv`, `messages_*.json`"),
    server_port: int = Field(445, description="Smb server port"),
    my_name: str = Field("", description="Client dns name"),
    remote_name: str = Field("", description="DNS name of the server"),
    ) -> str:
    """
    This tool is for getting a sample of a file's content shared on the SMB server.
    It gets the file list and reads the first 50 KB of either a specified file or the first file in the provided path.
    """
    try:
        conn = SMBConnection(username, password, my_name=my_name, remote_name=remote_name, use_ntlm_v2=True)
        conn.connect(server_ip, server_port)
        
        file_list = conn.listPath(share_name, path)

        for f in file_list:
            if isinstance(f, SharedFile) and fnmatch(f.filename, file_mask):
                file_obj = io.BytesIO()
                conn.retrieveFile(share_name, path + f.filename, file_obj)
                file_obj.seek(0)

                # Read the content
                file_content = file_obj.read()
                cp = chardetect(file_content)
                encoding = cp.get('encoding', 'utf-8') if isinstance(cp, dict) else cp
                content = file_content.decode(encoding=encoding)
                conn.close()
                return content[:50_000]

        conn.close()
        return 'Share or files not found'
    except Exception as e:
        return f"Error: {e}"
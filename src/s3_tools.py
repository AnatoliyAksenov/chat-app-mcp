import io
import s3fs
import boto3

import pandas as pd
import pyarrow.parquet as pq

from botocore.client import Config
from botocore.exceptions import ClientError

from fnmatch import fnmatch
from chardet import detect as chardetect
from fastmcp import FastMCP
from pathlib import Path

from pydantic import BaseModel, Field
from typing import Optional, Literal




mcp = FastMCP()


@mcp.tool
async def test_s3_connection(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection")
    ) -> str:
    """
    This tool is for checking connection to the s3 like service
    """
    try:
        if region:
            conf = Config(signature_version="s3v4", region_name=region)
        else:
            conf = Config(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        client = boto3.client(
                "s3",
                endpoint_url=url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=conf,
                **verify_conf
            )
        
        buckets = client.list_buckets()
        
        return 'Success'
    except Exception as e:
        return str(e)


@mcp.tool
async def test_s3_bucket_exists(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    bucket_name: str = Field(..., description="Bucket name for s3 like file storage"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection"),
    prefix: str = Field("", description="Folder in bucket")
    ) -> str:
    """
    This tool is for checking if the bucket in s3 like service exists and prefix contains something
    """
    try:
        if region:
            conf = Config(signature_version="s3v4", region_name=region)
        else:
            conf = Config(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        client = boto3.client(
                "s3",
                endpoint_url=url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=conf,
                **verify_conf
            )
        
        paginator = client.get_paginator('list_objects_v2')
    
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    return 'Success'
            else:
                return f"No objects found in bucket '{bucket_name}' with prefix '{prefix}'."
    
            
        return 'Bucket does not exists'
    except Exception as e:
        return str(e)


@mcp.tool
async def get_s3_bucket_object_list(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    bucket_name: str = Field(..., description="Bucket name for s3 like file storage"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection"),
    prefix: str = Field("", description="Folder in bucket")
    ) -> str:
    """This tool is for getting object list from s3 like service
    """
    try:
        if region:
            conf = Config(signature_version="s3v4", region_name=region)
        else:
            conf = Config(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        client = boto3.client(
                "s3",
                endpoint_url=url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=conf,
                **verify_conf
            )
        
        paginator = client.get_paginator('list_objects_v2')
    
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        content = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    print(obj)
                    content.append(obj)
            else:
                return f"No objects found in bucket '{bucket_name}' with prefix '{prefix}'."
        
        if content:
            return "\n".join([F"{x['Key']}\t{x['Size']}" for x in content])
            
        return 'Bucket does not exists'
    except Exception as e:
        return str(e)


#@mcp.tool
async def get_s3_bucket_object_sample_old(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    bucket_name: str = Field(..., description="Bucket name for s3 like file storage"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection"),
    prefix: str = Field("", description="Folder in bucket"),
    file_or_mask: str = Field("*", description="File name or mask to get files")
    ) -> str:
    """
    This tool is for getting object sampel from s3 like service. It gets obect list and reads 50kb of the specified or first object.
    """
    try:
        if region:
            conf = Config(signature_version="s3v4", region_name=region)
        else:
            conf = Config(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
        )
             
        resource = session.resource(
            's3',
            endpoint_url=url,
            config=conf,
            **verify_conf
        )
        bucket = resource.Bucket(bucket_name)
        
        # Filter by prefix if provided
        content = ''
        if prefix:
            for obj in bucket.objects.filter(Prefix=prefix):
                if fnmatch(obj.key, file_or_mask):
                    break

        else:
            for obj in bucket.objects.all():
                if fnmatch(obj.key, file_or_mask):
                    break

        if obj:
            meta = obj.get()
            body = meta.get('Body')
            content = body.read(50_000)
            cp = chardetect(content)
            encoding = cp.get('encoding', 'utf-8') if isinstance(cp, dict) else cp
            sample = content.decode(encoding=encoding)
            return sample
        
        return ''
    except Exception as e:
        return str(e)
    

@mcp.tool
async def get_s3_bucket_object_sample(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    bucket_name: str = Field(..., description="Bucket name for s3 like file storage"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection"),
    prefix: str = Field("", description="Folder in bucket"),
    file_or_mask: str = Field("*", description="File name or mask to get files")
    ) -> str:
    """
    This tool is for getting object sample from s3 like service. It gets obect list and reads 50kb of the specified or first object. It supports parquet, csv, tsv and any text formats.
    """
    try:
        if region:
            conf = dict(signature_version="s3v4", region_name=region)
        else:
            conf = dict(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        conf.update(verify_conf)

        fs = s3fs.S3FileSystem(endpoint_url=url, key=access_key, secret=secret_key, config_kwargs=conf, anon=False)

        for f in fs.ls(bucket_name+prefix):
            ff = Path(f)
            if ff.match(file_or_mask):
                try:
                    if ff.suffix.lower() == '.parquet':
                        df = pd.read_parquet(f"s3://{f}", filesystem=fs)

                        return df.sample(n=100).to_markdown(index=False)
                    
                    elif ff.suffix.lower() in ['.csv', '.tsv']:
                        content = fs.read_block(f, 0, 50_000)
                        buf = io.BytesIO()
                        buf.write(content)
                        buf.seek(0)
                        sep = ',' if ff.suffix.lower() == '.csv' else '\t'
                        df = pd.read_csv(buf, sep=sep)

                        return df.iloc[:-1].sample(n=100).to_markdown(index=False)    
                    
                    else:
                        content = fs.read_block(f, 0, 50_000)
                        cp = chardetect(content)
                        encoding = cp.get('encoding', 'utf-8') if isinstance(cp, dict) else cp
                        data = content.decode(encoding=encoding)

                        return data
                    
                except Exception as e:
                    print(e)
                    return fs.read_block(f, 0, 50_000)
        
        return ''
    except Exception as e:
        return str(e)
    

@mcp.tool
async def get_s3_bucket_parquet_schema(
    url: str = Field(..., description="Server url. Have to have protocol(http or https), host and port"),
    access_key: str = Field(..., description="Access key"),
    secret_key: str = Field(..., description="Secret key"),
    bucket_name: str = Field(..., description="Bucket name for s3 like file storage"),
    region: str = Field(None, description="Region name"),
    verify: bool = Field(None, description="Verify ssl connection"),
    prefix: str = Field("", description="Folder in bucket"),
    file_or_mask: str = Field("*", description="File name or mask to get files")
    ) -> dict:
    """This tool returns parquet schema from s3 like service. It gets obect list and reads the specified or first object.
    """
    try:
        if region:
            conf = dict(signature_version="s3v4", region_name=region)
        else:
            conf = dict(signature_version="s3v4")

        if verify is None:
            verify_conf = {}
        else:
            verify_conf = {"verify_conf": verify}

        conf.update(verify_conf)

        fs = s3fs.S3FileSystem(endpoint_url=url, key=access_key, secret=secret_key, config_kwargs=conf, anon=False)

        for f in fs.ls(bucket_name+prefix):
            ff = Path(f)
            if ff.match(file_or_mask):
                try:
                    if ff.suffix.lower() == '.parquet':
                        parquet_file = pq.ParquetFile(f, filesystem=fs)

                        schema = parquet_file.schema_arrow
                        schema_dict = {name: str(typ) for name, typ in zip(schema.names, schema.types)}
                        return schema_dict
                    
                    else:
                        
                        return {}
                    
                except Exception as e:

                    return {'error':{str(e)}}
        
        return {}
    except Exception as e:
        return str(e)
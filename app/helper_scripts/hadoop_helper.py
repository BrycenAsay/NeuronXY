import pyarrow.parquet as pq
import pyarrow.csv as csv
import pandas as pd
import pyarrow as pa
from typing import Literal
import httpx
import logging
import io

def chown(path: str, owner: str, group: str = 'admin'):
    """Change ownership."""
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            url = f"{nn}/webhdfs/v1/{path}?op=SETOWNER&owner={owner}&group={group}&user.name=root"
            with httpx.Client() as client:
                response = client.put(url)
                response.raise_for_status()
        except Exception:
            continue

def stupid_create_dir(full_path):
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            url = f"{nn}/webhdfs/v1/{full_path}?op=MKDIRS&user.name=root"
                
            with httpx.Client() as client:
                response = client.put(url)
                response.raise_for_status()
        except Exception:
            continue

def create_hdfs_directory(full_path, user, file_upload=False):

    paths = full_path.split('/') 
    all_dirs = []
    for i in range(1, len(paths)):
        all_dirs.append('/'.join(paths[0:i]))
    if not file_upload:
        all_dirs.append(full_path)
    for dir in all_dirs:
        try:
            stupid_create_dir(dir)
            if dir not in ['cortex', '/cortex']: # keep a list of directories we do NOT want to be owned by individual users
                chown(dir, user)
        except FileExistsError:
            pass

def delete_hdfs_directory(full_path, user):
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            url = f"{nn}/webhdfs/v1/{full_path}?op=DELETE&user.name={user}&recursive=true"
                
            with httpx.Client() as client:
                response = client.delete(url)
                response.raise_for_status()
            input('Did we make it? ')
        except Exception:
            continue

def delete_hdfs_file(full_path, user):
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            url = f"{nn}/webhdfs/v1/{full_path}?op=DELETE&user.name={user}"
                
            with httpx.Client() as client:
                response = client.delete(url)
                response.raise_for_status()
        except Exception:
            continue

def upload_hdfs_file(local_path, hdfs_path, user):
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            # Step 1: Create file (get redirect)
            create_url = f"{nn}/webhdfs/v1{hdfs_path}?op=CREATE&user.name={user}&overwrite=true"
            
            with httpx.Client() as client:
                response = client.put(create_url, follow_redirects=False)
                
                if response.status_code == 307:
                    # Step 2: Upload data to datanode
                    upload_url = response.headers['Location']
                    
                    with open(local_path, 'rb') as f:
                        upload_response = client.put(upload_url, content=f)
                        upload_response.raise_for_status()
                        break
        except Exception as e:
            logging.error(e)

def upload_pa_table(data, hdfs_dest_path, user):
    import tempfile
    import os
    
    # Convert to PyArrow Table if input is a pandas DataFrame
    if isinstance(data, pd.DataFrame):
        table = pa.Table.from_pandas(data)
    elif isinstance(data, pa.Table):
        table = data
    else:
        raise TypeError("Data must be either a pandas DataFrame or a PyArrow Table")
    
    # Force .parquet extension if not already
    if not hdfs_dest_path.endswith('.parquet'):
        hdfs_dest_path = hdfs_dest_path.rsplit('.', 1)[0] + '.parquet'
    
    # Write table to temporary local file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
        tmp_path = tmp.name
    
    try:
        pq.write_table(table, tmp_path)
        upload_hdfs_file(tmp_path, hdfs_dest_path, user)
    finally:
        os.remove(tmp_path)

def read_hdfs_file(hdfs_file_path, user, file_type:Literal['pq', 'csv', 'xlsx'], output_format:Literal['pandas_df', 'arrow_table']):
    namenodes = ["http://namenode1:9870", "http://namenode2:9870"]
    
    for nn in namenodes:
        try:
            # Step 1: Create file (get redirect)
            create_url = f"{nn}/webhdfs/v1{hdfs_file_path}?op=OPEN&user.name={user}"
            
            with httpx.Client() as client:
                response = client.get(create_url, follow_redirects=False)
                
                if response.status_code == 307:
                    # Step 2: Upload data to datanode
                    read_url = response.headers['Location']
                    download_response = client.get(read_url)
                    download_response.raise_for_status()
                    content = download_response.content
        except Exception as e:
            logging.error(e)
    
    if content is None:
        raise Exception(f"Failed to download {hdfs_file_path} from HDFS")
    
    # Read based on file type and output format
    if file_type == 'pq':
        table = pq.read_table(io.BytesIO(content))
        if output_format == 'pandas_df':
            return table.to_pandas()
        elif output_format == 'arrow_table':
            return table
    elif file_type == 'csv' and output_format == 'pandas_df':
        return pd.read_csv(io.BytesIO(content))
    elif file_type == 'xlsx' and output_format == 'pandas_df':
        return pd.read_excel(io.BytesIO(content))
    elif file_type == 'csv' and output_format == 'arrow_table':
        return csv.read_csv(io.BytesIO(content))
    else:
        print(f"ERROR! Format {output_format} does not support reading file type {file_type}! Please choose a different file type/format combination!")
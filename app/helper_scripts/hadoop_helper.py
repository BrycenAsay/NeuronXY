from pyarrow.fs import HadoopFileSystem
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pandas as pd
import pyarrow as pa
from typing import Literal
import io

def create_hdfs_direcotry(full_path):
    paths = full_path.split('/') 
    all_dirs = []
    for i in range(1, len(paths)):
        all_dirs.append('/'.join(paths[0:i]))
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    for dir in all_dirs:
        try:
            hdfs.create_dir(dir)
        except FileExistsError:
            pass

def delete_hdfs_direcotry(full_path):
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    try:
        hdfs.delete_dir(full_path)
    except FileNotFoundError:
        pass

def delete_hdfs_file(full_path):
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    try:
        hdfs.delete_file(full_path)
    except FileNotFoundError:
        pass

def upload_hdfs_file(local_file_path, hdfs_dest_path):
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    create_hdfs_direcotry(hdfs_dest_path)

    with open(local_file_path, 'rb') as local_file:
        with hdfs.open_output_stream(hdfs_dest_path) as hdfs_file:
            hdfs_file.write(local_file.read())

def upload_pa_table(data, hdfs_dest_path):
    # Connect to HDFS
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    
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

    # Write the table to HDFS as Parquet
    with hdfs.open_output_stream(hdfs_dest_path) as f:
        pq.write_table(table, f)

def read_hdfs_file(hdfs_file_path, file_type:Literal['pq', 'csv', 'xlsx'], output_format:Literal['pandas_df', 'arrow_table']):
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    if file_type == 'pq':
        table = pq.read_table(hdfs_file_path, filesystem=hdfs)
        if output_format == 'pandas_df':
            return table.to_pandas()
        elif output_format == 'arrow_table':
            return table
    with hdfs.open_input_file(hdfs_file_path) as hdfs_file:
        content = hdfs_file.read()
        if (file_type == 'csv' and output_format == 'pandas_df'):
            return pd.read_csv(io.BytesIO(content))
        elif (file_type == 'xlsx' and output_format == 'pandas_df'):
            return pd.read_excel(io.BytesIO(content))
        elif (file_type == 'csv' and output_format == 'arrow_table'):
            return csv.read_csv(io.BytesIO(content))
        else:
            print(f"ERROR! Format {output_format} does not support reading file type {file_type}! Please choose a different file type/format combination!")
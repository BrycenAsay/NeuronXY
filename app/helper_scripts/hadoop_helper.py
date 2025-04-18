from pyarrow.fs import HadoopFileSystem
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
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

def read_hdfs_file(hdfs_file_path, file_type):
    hdfs = HadoopFileSystem.from_uri("hdfs://mycluster")
    if file_type == 'pq':
        table = pq.read_table(hdfs_file_path, filesystem=hdfs)
        final_df = table.to_pandas()
    try:
        with hdfs.open_input_file(hdfs_file_path) as hdfs_file:
            content = hdfs_file.read()
            if file_type == 'csv':
                final_df = pd.read_csv(io.BytesIO(content))
            elif file_type == 'xlsx':
                final_df = pd.read_excel(io.BytesIO(content))
        return final_df
    except:
        pass

from pyarrow.fs import HadoopFileSystem

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
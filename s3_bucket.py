from datetime import datetime
import random
import os
import pytz
import shutil
from sql_helper import create_row, create_db_connection, name_to_id

class s3_object:
    def __init__(self,
                properties=['uri', 'arn', 'version', 'etag', 'object_url', 'owner', 'last_modified', 'size', 'type', 'storage_class', 'tags'],
                uri='', 
                arn='',
                version=0, 
                etag='', 
                object_url='', 
                owner='', 
                last_modified='', 
                size='', 
                type='', 
                storatge_class='standard',
                tags=''):
        self.properties = properties
        self.uri = uri
        self.arn = arn
        self.version = version
        self.etag = etag
        self.object_url = object_url
        self.owner = owner
        self.last_modified = last_modified
        self.size = size
        self.type = type
        self.storage_class = storatge_class
        self.tags = tags.split(',')
        
    def get_object_properties(self, attr):
        if attr == 'uri':
            return self.uri
        if attr == 'arn':
            return self.arn
        if attr == 'version':
            return self.version
        if attr == 'etag':
            return self.etag
        if attr == 'object_url':
            return self.object_url
        if attr == 'owner':
            return self.owner
        if attr == 'last_modified':
            return self.last_modified
        if attr == 'size':
            return self.size
        if attr == 'type':
            return self.type
        if attr == 'storage_class':
            return self.storage_class
        if attr == 'tags':
            return self.tags

    @staticmethod
    def validate_value(value, object_attr):
        if object_attr == 'storage_class':
            if value not in ['standard']:
                print('ERROR! VALUE IS NOT VALID!')
                return False
            else:
                return True

def version_bucket(bucket):
    if bucket.bucket_versioning:
        return 1 # will build in versioning logic later
    else:
        return 0

def etag_create():
    sequence = []
    for i in range(32):
        num_or_letter = random.randint(1, 2)
        if num_or_letter == 1:
            sequence.append(random.randint(48, 57))
        elif num_or_letter == 2:
            upper_or_lower = random.randint(1, 2)
            if upper_or_lower == 1:
                sequence.append(random.randint(65, 90))
            elif upper_or_lower == 2:
                sequence.append(random.randint(97, 122))
    return ''.join([chr(x) for x in sequence])

def get_file_size_in_units(file_path):
    file_size_bytes = os.path.getsize(file_path)
    
    # Convert bytes to KB, MB, GB, etc.
    if file_size_bytes < 1024:
        return f"{file_size_bytes} bytes"
    elif file_size_bytes < 1024 ** 2:
        return f"{file_size_bytes / 1024:.2f} KB"
    elif file_size_bytes < 1024 ** 3:
        return f"{file_size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{file_size_bytes / (1024 ** 3):.2f} GB"

def persist_object(_bucket, username, object_name, object_path):
    cols = ['user_id', 'bucket_id']
    user_id = name_to_id('user_credentials', 'user_id', 'username', username)
    bucket_id = name_to_id('s3', 'bucket_id', 'name', _bucket.name)
    prepro_vals = [user_id, bucket_id]
    vals = []
    new_object = s3_object(
        uri=f's3://{_bucket.name}/{object_name}',
        arn=f'arn:aws:s3:::{_bucket.name}/{object_name}',
        version = version_bucket(_bucket),
        etag= etag_create(),
        object_url = f'{object_path}{object_name}',
        owner = username,
        last_modified = datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None),
        size = get_file_size_in_units(f'{object_path}{object_name}'),
        type = object_name.split('.')[1])
    for attribute in new_object.properties:
        cols.append(attribute)
        prepro_vals.append(new_object.get_object_properties(attribute))
    for val in prepro_vals:
        if ((isinstance(val, str)) or (isinstance(val, datetime))):
            vals.append(f"'{str(val)}'")
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}")
        else:
            vals.append(str(val))
    create_db_connection(create_row('s3_bucket', cols, vals))

def upload_object(_username, bucket, object_name):
    source = f"AWS/externalFiles/{object_name}"
    destination = f"AWS/users/{_username}/s3/{bucket.name}/"
    try:
        shutil.move(source, destination)
        persist_object(bucket, _username, object_name, destination)
    except:
        print(f'ERROR! No object under name "{object_name}" found! Please ensure object is in the "externalFiles" directory or ensure object name is spelled correctly!')
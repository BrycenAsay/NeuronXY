from datetime import datetime
import random
import os
import pytz
import shutil
from sql_helper import create_row, create_db_connection, name_to_id

class s3_object:
    """s3 object object (yea I said object twice X), attributes for objects within a bucket currently include:
    
    properties: a list of properties for this object
    uri: the uniform resource identifier for the object
    arn: amazon resource name for the object
    version: object version (will be 0 if versioning is disabled on the bucket)
    etag: unique 32 character identifier for the object
    object_url: file path for the object (In actual AWS, this would be a publicly searchable URL)
    owner: owner of the object in the bucket
    size: the size of the object, in bytes, KB, MG, or GB
    type: file type of object
    storage_class: storage class for the object
    tags: list of tags associated with the bucket
    """
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
        """Returns properties of object given attribute in string form as a function parameter"""
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
        """Validates that the value being set for an attribute is valid where validation is needed, returns
        False where the validation criteria is not met"""
        if object_attr == 'storage_class':
            if value not in ['standard']:
                print('ERROR! VALUE IS NOT VALID!')
                return False
            else:
                return True

def version_bucket(bucket):
    """Will version an object depending on the ordinal value of the object upload"""
    if bucket.bucket_versioning:
        return 1 # will build in versioning logic later
    else:
        return 0

def etag_create():
    """Creates unique 32 character etag identifier"""
    sequence = []
    for i in range(32):
        num_or_Ll = [random.randint(1, 2), random.randint(1, 2)]
        if num_or_Ll[0] == 1:
            sequence.append(random.randint(48, 57))
        elif (num_or_Ll[0] == 2) and (num_or_Ll[1] == 1):
            sequence.append(random.randint(65, 90))
        elif (num_or_Ll[0] == 2) and (num_or_Ll[1] == 2):
            sequence.append(random.randint(97, 122))
    return ''.join([chr(x) for x in sequence])

def get_file_size_in_units(file_path):
    """Gets size of object in bytes, KB, MB, or GB depending on which measure is the most practical for the given object"""
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
    """Creates an instance of s3_object object, loads data according to the object uploaded, then after defining all
    object attributes, uploads those attributes to the DB"""
    cols = ['user_id', 'bucket_id']
    user_id = name_to_id('user_credentials', 'user_id', 'username', username) #retrieve user_id from DB based off username
    bucket_id = name_to_id('s3', 'bucket_id', 'name', _bucket.name) #retrieve bucket_id from DB based off bucket name
    prepro_vals = [user_id, bucket_id]
    vals = []
    new_object = s3_object( #instaniate s3_object and define values based on object information
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
        cols.append(attribute) #columns to update in DB, matched with object attributes
        prepro_vals.append(new_object.get_object_properties(attribute)) #values to uploaded, pre postgres friendly translation
    # go through the unprocessed values, and change formating to postgres friendly as per value data type
    for val in prepro_vals:
        if ((isinstance(val, str)) or (isinstance(val, datetime))):
            vals.append(f"'{str(val)}'")
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}::TEXT[]")
        else:
            vals.append(str(val))
    create_db_connection(create_row('s3_bucket', cols, vals))

def upload_object(_username, bucket, object_name):
    """Uploads object from externalFiles folder into an s3 bucket and records details in the DB"""
    source = f"AWS/externalFiles/{object_name}"
    destination = f"AWS/users/{_username}/s3/{bucket.name}/"
    try: # attempt to find object name in externalFiles and move object to subdirectory. Afterwards, record object details in DB
        shutil.move(source, destination)
        persist_object(bucket, _username, object_name, destination)
    except: # if object is not found, throw error that object could not be found
        print(f'ERROR! No object under name "{object_name}" found! Please ensure object is in the "externalFiles" directory or ensure object name is spelled correctly!')
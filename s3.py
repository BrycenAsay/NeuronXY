import json
import random
from datetime import datetime
import pytz
import logging
import os
from sql_helper import create_row, create_db_connection, delete_row_two_ids, update_row_dos_id, name_to_id
from sqlalchemy import text

y_or_n_input = lambda x: True if x == 'Y' else False

class s3_bucket:
    def __init__(self,
                 properties = ['name', 'bucket_type', 'bucket_versioning', 'acl_enabled', 
                               'block_public_access', 'bucket_key', 'object_lock', 'encrypt_method',
                               'bucket_policy', 'tags'],
                 name = '',
                 bucket_type = 'gp', 
                 bucket_versioning:bool=False, 
                 acl_enabled:bool=False, 
                 block_public_access:bool=True, 
                 bucket_key:bool=True,
                 object_lock:bool=False,
                 encrypt_method='SSE-S3', 
                 bucket_policy=json.dumps({}), 
                 tags=[]):
        self.properties = properties
        self.name = name
        self.block_public_access = block_public_access
        self.acl_enabled = acl_enabled
        self.bucket_policy = bucket_policy
        self.bucket_type = bucket_type
        self.bucket_versioning = bucket_versioning
        self.tags = tags
        self.encrypt_method = encrypt_method
        self.bucket_key = bucket_key
        self.object_lock = object_lock

    def set_bucket_properties(self, attr):
        if attr == 'name':
            self.name = input('Please enter a name for this bucket between 3 to 63 characters> ')
        if attr == 'block_public_access':
            self.block_public_access = y_or_n_input(input('Would you like to block public access? (Y/N)> '))
        if attr == 'acl_enabled':
            self.acl_enabled = y_or_n_input(input('Would you like to enable ACLs? (Y/N)> '))
        if attr == 'bucket_policy':
            try:
                self.bucket_policy = json.dumps(input('Please enter a bucket policy> '))
            except:
                self.bucket_policy = 'NOT A VALID JSON OBJECT'
        if attr == 'bucket_type':
            self.bucket_type = input('Please enter a valid bucket type (gp or dir)> ')
        if attr == 'bucket_versioning':
            self.bucket_versioning = y_or_n_input(input('Would you like to enable bucket versioning? (Y/N)> '))
        if attr == 'tags':
            tags = input('Please enter tags you want associated with the bucket, seperated by commas> ').split(',')
            if '' in tags:
                tags.remove('')
            self.tags = tags
        if attr == 'encrypt_method':
            self.encrypt_method = input('Please enter a valid encryption method (SSE-S3, SEE-KMS, DSSE-KMS)> ')
        if attr == 'bucket_key':
            self.bucket_key = y_or_n_input(input('Would you like to create a bucket key? (Y/N)> '))
        if attr == 'object_lock':
            self.object_lock = y_or_n_input(input('Would you like to enable object lock? (Y/N)> '))

    def define_bucket_properties(self, attr, value):
        if attr == 'name':
            self.name = value
        if attr == 'block_public_access':
            self.block_public_access = value
        if attr == 'acl_enabled':
            self.acl_enabled = value
        if attr == 'bucket_policy':
            self.bucket_policy = json.dumps(value)
        if attr == 'bucket_type':
            self.bucket_type = value
        if attr == 'bucket_versioning':
            self.bucket_versioning = value
        if attr == 'tags':
            self.tags = value
        if attr == 'encrypt_method':
            self.encrypt_method = value
        if attr == 'bucket_key':
            self.bucket_key = value
        if attr == 'object_lock':
            self.object_lock = value
        
    def get_bucket_properties(self, attr):
        if attr == 'name':
            return self.name
        if attr == 'block_public_access':
            return self.block_public_access
        if attr == 'acl_enabled':
            return self.acl_enabled
        if attr == 'bucket_policy':
            return self.bucket_policy
        if attr == 'bucket_type':
            return self.bucket_type
        if attr == 'bucket_versioning':
            return self.bucket_versioning
        if attr == 'tags':
            return self.tags
        if attr == 'encrypt_method':
            return self.encrypt_method
        if attr == 'bucket_key':
            return self.bucket_key
        if attr == 'object_lock':
            return self.object_lock

    @staticmethod
    def validate_value(value, object_attr):
        if object_attr == 'bucket_type':
            if value not in ['gp', 'dir']:
                print('ERROR! VALUE MUST EITHER BE GP (GENERAL PURPOSE) OR DIR (DIRECTORY)')
                return False
            else:
                return True
        if object_attr == 'encrypt_method':
            if value not in ['SSE-S3', 'SEE-KMS', 'DSSE-KMS']:
                print('ERROR! VALID ENCRYPTION SETTING NOT SELECTED! PLEASE SELECT A VALID ENCRYPTION SETTING!')
                return False
            else:
                return True
        if object_attr == 'name':
            if (3 <= len(value) <= 63) is False:
                print('ERROR! NAME MUST BE BETWEEN 3 AND 63 CHARACTERS!')
                return False
            elif (1 in [x[0] for x in create_db_connection(text(f"SELECT 1 FROM s3 WHERE name = '{value}'"), return_result=True)]):
                print('ERROR! Bucket name is already in use by an existing user. Please choose a different bucket name!')
                return False
            else:
                return True
        if object_attr == 'bucket_policy':
            try:
                parsed_data = json.loads(value)
                return True
            except:
                print('ERROR! VALUE IS NOT IN VALID JSON FORMAT!')
                return False
        else:
            return True

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

def set_vv_abap(bucket, _attr):
    bucket.set_bucket_properties(_attr)
    while bucket.validate_value(bucket.get_bucket_properties(_attr), _attr) != True:
        bucket.set_bucket_properties(_attr)
    return bucket

def upload_properties_to_db(bucket, _username):
    cols = ['user_id'] + bucket.properties
    prepro_vals = [x[0] for x in create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)]
    vals = []
    for property in bucket.properties:
        prepro_vals.append(bucket.get_bucket_properties(property))
    for val in prepro_vals:
        if isinstance(val, str):
            vals.append(f"'{str(val)}'")
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}")
        else:
            vals.append(str(val))
    update_statement = create_row('s3', cols, vals)
    create_db_connection(update_statement)

def remove_bucket_db_dir(_user_id, _bucket_id):
    create_db_connection(delete_row_two_ids('s3', 'user_id', _user_id, 'bucket_id', _bucket_id))

def update_bucket(_user_id, _bucket_id):
    existing_bucket = s3_bucket()
    for attribute in existing_bucket.properties:
        existing_bucket.define_bucket_properties(attribute, [x[0] for x in create_db_connection(text(f"SELECT {attribute} FROM s3 WHERE user_id = {_user_id} AND bucket_id = {_bucket_id}"), return_result=True)][0])
    
    updateable_properties = existing_bucket.properties
    updateable_properties.remove('name')
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change in updateable_properties:
            existing_bucket = set_vv_abap(existing_bucket, def_pv_to_change)
        else:
            pass
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')

    for property in existing_bucket.properties:
        val = existing_bucket.get_bucket_properties(property)
        if isinstance(val, str):
            val = f"'{str(val)}'"
        elif isinstance(val, list):
            val = f"ARRAY{val}"
        else:
            val = str(val)
        create_db_connection(update_row_dos_id('s3', property, val, 'user_id', _user_id, 'bucket_id', _bucket_id))

def create_s3_directory(_username, bucket):
    s3_sub_path = f"AWS/users/{_username}/s3"
    try:
        os.makedirs(s3_sub_path, exist_ok=False)
    except:
        pass
    bucket_sub_path = f"AWS/users/{_username}/s3/{bucket.name}"
    os.makedirs(bucket_sub_path, exist_ok=True)

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

if __name__ == '__main__':
    pass
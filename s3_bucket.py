from datetime import datetime
import random
import os
import pytz
import shutil
from sql_helper import create_row, create_db_connection, name_to_id, row_action, update_row
from s3 import sel_bucket

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
                properties=['uri', 'arn', 'sub_version_id', 'version_id', 'etag', 'object_url', 'owner', 'last_modified', 'size', 'type', 'storage_class', 'tags'],
                uri='', 
                arn='',
                sub_version_id=0,
                version_id='', 
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
        self.sub_version_id = sub_version_id
        self.version_id = version_id
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
        if attr == 'sub_version_id':
            return self.sub_version_id
        if attr == 'version_id':
            return self.version_id
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

def version_bucket(_username, bucket, object_name):
    """Will version an object depending on the ordinal value of the object upload"""
    if bucket.bucket_versioning:
        obj_sub_vers_ids = create_db_connection(row_action('s3_bucket', ['user_id', 'bucket_id', 'arn'], 
                                                        [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                         name_to_id('s3', 'bucket_id', 'name', bucket.name),
                                                         f"'arn:aws:s3:::{bucket.name}/{object_name}'"], 'SELECT sub_version_id'), return_result = True)
        if (0 in obj_sub_vers_ids or 1 in obj_sub_vers_ids):
            return [max(obj_sub_vers_ids) + 1, vers_id_create()]
        else:
            return [1, vers_id_create()]
    else:
        return [0, '']

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

def vers_id_create():
    """Creates unique 32 character version identifier"""
    sequence = []
    for i in range(32):
        cap_or_low = random.randint(1, 2)
        if cap_or_low == 1:
            sequence.append(random.randint(65, 90))
        elif cap_or_low == 2:
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
    version_vals = version_bucket(username, _bucket, object_name)
    new_object = s3_object( #instaniate s3_object and define values based on object information
        uri=f's3://{_bucket.name}/{object_name}',
        arn=f'arn:aws:s3:::{_bucket.name}/{object_name}',
        sub_version_id = version_vals[0],
        version_id = version_vals[1],
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
            if val != 'Null':
                vals.append(f"'{str(val)}'")
            else:
                vals.append(val)
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}::TEXT[]")
        else:
            vals.append(str(val))
    create_db_connection(create_row('s3_bucket', cols, vals))

def object_replication(_username, _bucket, _object_name, _perm_tag):
    replicate_upload_to_bnm = create_db_connection(row_action('s3', ['replication_bucket_id'], [name_to_id('s3', 'bucket_id', 'name', _bucket.name)], action_type='SELECT name'), return_result=True)
    for bname in replicate_upload_to_bnm:
        bucket_rep_to = sel_bucket(_username, bname, None, True)
        upload_object(_username, bucket_rep_to, _object_name, None, True)

def upload_object(_username, bucket, object_name, perm_tag, replicate_process=False):
    """Uploads object from externalFiles folder into an s3 bucket and records details in the DB"""
    source = f"AWS/externalFiles/{object_name}"
    destination = f"AWS/users/{_username}/s3/{bucket.name}/"
    try: # attempt to find object name in externalFiles and move object to subdirectory. Afterwards, record object details in DB
        shutil.copy(source, destination)
        obj_exists = create_db_connection(row_action('s3_bucket', ['user_id', 'bucket_id', 'arn'], 
                                                        [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                         name_to_id('s3', 'bucket_id', 'name', bucket.name),
                                                         f"'arn:aws:s3:::{bucket.name}/{object_name}'"], 'SELECT 1'), return_result = True)
        if ((bucket.bucket_versioning and 1 in obj_exists) or obj_exists == []):
            persist_object(bucket, _username, object_name, destination)
            if not replicate_process:
                object_replication(_username, bucket, object_name, None)
        else:
            create_db_connection(row_action('', ['user_id', 'bucket_id', 'arn'], 
                                                [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                name_to_id('s3', 'bucket_id', 'name', bucket.name),
                                                f"'arn:aws:s3:::{bucket.name}/{object_name}'"], f"UPDATE s3_bucket SET last_modified = '{datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None)}'", frm_keywrd=''))
    except: # if object is not found, throw error that object could not be found
        print(f'ERROR! No object under name "{object_name}" found! Please ensure object is in the "externalFiles" directory or ensure object name is spelled correctly!')

def delete_object(_username, bucket, object_name, perm_tag:bool=False):
    """Removes object from DB and bucket directory"""
    source = f"AWS/users/{_username}/s3/{bucket.name}/{object_name}"
    if (bucket.bucket_versioning and not perm_tag): # if bucket versioning is enabled and delete is not specified as permenent
        sub_version_ids = create_db_connection(row_action('s3_bucket', ['user_id', 'bucket_id', 'arn'], 
                                                            [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                            name_to_id('s3', 'bucket_id', 'name', bucket.name),
                                                            f"'arn:aws:s3:::{bucket.name}/{object_name}'"], 'SELECT object_id, sub_version_id, version_id', order_state='ORDER BY sub_version_id DESC'), multi_return=[True, 3])
        if sub_version_ids[0] == []: # if no results returned, object does not exist
            print(f'ERROR! No object under name "{object_name}" found! Please ensure object exists in bucket {bucket.name} or ensure object name is spelled correctly!')
        elif (sub_version_ids[2][0] == 'delete-marker'): # if there is  delete marker and bucket is versioned, you must specifiy a permenanet delete for a delete to take place
            print(f'ERROR! Final object version already marked as deleted, please use the --perm tag or disable versioning if you wish to permenantly delete this object!')
        elif sub_version_ids[1][0] == sub_version_ids[1][-1]: # this indicates that there is only the oldest version still avaliable in the DB. Update version to a delete-marker
            create_db_connection(row_action('', ['user_id', 'bucket_id', 'object_id'], 
                                                [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                name_to_id('s3', 'bucket_id', 'name', bucket.name),
                                                sub_version_ids[0][0]], f"UPDATE s3_bucket SET version_id = 'delete-marker'", frm_keywrd=''))
        elif sub_version_ids[1][0] > sub_version_ids[1][-1]: # delete the latest version if there is more than one version of an object
            create_db_connection(row_action('s3_bucket', ['user_id', 'bucket_id', 'object_id'], [name_to_id('user_credentials', 'user_id', 'username', _username),
            name_to_id('s3', 'bucket_id', 'name', bucket.name), sub_version_ids[0][0]], 'DELETE'))
    else: # perm delete or delete with bucket versioning disabled
        try: # attempt to find object name in externalFiles and move object to subdirectory. Afterwards, record object details in DB
            os.remove(source)
            create_db_connection(row_action('s3_bucket', ['user_id', 'bucket_id', 'arn'], [name_to_id('user_credentials', 'user_id', 'username', _username),
            name_to_id('s3', 'bucket_id', 'name', bucket.name), f"'arn:aws:s3:::{bucket.name}/{object_name}'"], 'DELETE'))
        except: # if object is not found, throw error that object could not be found
            print(f'ERROR! No object under name "{object_name}" found! Please ensure object exists in bucket {bucket.name} or ensure object name is spelled correctly!')
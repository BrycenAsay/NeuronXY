import json
import shutil
import os
from sql_helper import create_row, create_db_connection, row_action, update_row_dos_id, name_to_id
from sqlalchemy import text

y_or_n_input = lambda x: True if x == 'Y' else False # returns true if value is 'Y' (yes) otherwise defaults to False

class s3_bucket:
    """s3 bucket object, attributes for a bucket currently include:
    
    properties: a list of properties for this object
    name: the name for the bucket
    block_public_access: dictates wether the bucket is or is not publicly accessible
    acl_enabled: dicates wether or not access control lists are endabled
    bucket_policy: dictates permissions for specific s3 bucket actions
    bucket_type: wether bucket is general purpose or directory
    bucket_versioning: dictates wether or not object versions are kept track of
    tags: list of tags associated with the bucket
    encrypt_method: dictates the encryption method for objects within the bucket
    bucket_key: a local bucket key that that lowers calls to AWS KMS
    object_lock: prevents objects from being deleted or overwritten when specified
    """
    def __init__(self,
                 properties = ['name', 'arn', 'bucket_type', 'bucket_versioning', 'acl_enabled', 
                               'block_public_access', 'bucket_key', 'object_lock', 'encrypt_method',
                               'bucket_policy', 'tags'],
                 name = '',
                 arn = '',
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
        self.arn = arn
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
        """Allows a terminal user to set bucket properties by passing the attribute string as the attr argument"""
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
        """Allows defining of bucket properties that are already known without prompting the user"""
        if attr == 'name':
            self.name = value
        if attr == 'arn':
            self.arn = value
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
        """returns the current bucket property value for a given attribute"""
        if attr == 'name':
            return self.name
        if attr == 'arn':
            return self.arn
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
        """Validates that the value being set for an attribute is valid where validation is needed, returns
        False where the validation criteria is not met"""
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
            elif (1 in create_db_connection(text(f"SELECT 1 FROM s3 WHERE name = '{value}'"), return_result=True)):
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

def set_vv_abap(bucket, _attr):
    """Sets and validates the attribute values for a bucket instance"""
    bucket.set_bucket_properties(_attr)
    while bucket.validate_value(bucket.get_bucket_properties(_attr), _attr) != True:
        bucket.set_bucket_properties(_attr)
    return bucket

def upload_properties_to_db(bucket, _username):
    """Uploads bucket properties to the DB"""
    cols = ['user_id'] + bucket.properties
    prepro_vals = create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)
    vals = []
    for property in bucket.properties:
        prepro_vals.append(bucket.get_bucket_properties(property))
    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query
    for val in prepro_vals:
        if isinstance(val, str):
            vals.append(f"'{str(val)}'")
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}::TEXT[]")
        else:
            vals.append(str(val))
    create_db_connection(create_row('s3', cols, vals))

def remove_bucket_db_dir(_user_id, _bucket_id):
    """deletes both the bucket row in the DB and removes the s3 bucket subdirectory"""
    create_db_connection(row_action('s3', ['user_id', 'bucket_id'], [_user_id, _bucket_id], 'DELETE'))

def update_bucket(_user_id, _bucket_id):
    """allows for users to update existing s3 bucket settings"""
    existing_bucket = s3_bucket() #instantiate s3_bucket instance
    for attribute in existing_bucket.properties: #pull bucket values from database and set s3_bucket attributes on exiting_bucket instance
        existing_bucket.define_bucket_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM s3 WHERE user_id = {_user_id} AND bucket_id = {_bucket_id}"), return_result=True)[0])
    
    # give user updatable properties (removed name, this is immutable after the bucket gets created) and prompt for values to change until user types 'DONE'
    updateable_properties = existing_bucket.properties
    updateable_properties.remove('name')
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change in updateable_properties:
            existing_bucket = set_vv_abap(existing_bucket, def_pv_to_change)
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update bucket values
    for property in existing_bucket.properties:
        val = existing_bucket.get_bucket_properties(property)
        if isinstance(val, str):
            val = f"'{str(val)}'"
        elif isinstance(val, list):
            val = f"ARRAY{val}::TEXT[]"
        else:
            val = str(val)
        create_db_connection(update_row_dos_id('s3', property, val, 'user_id', _user_id, 'bucket_id', _bucket_id))

def create_s3_directory(_username, bucket):
    """Creates an s3 directory if it does not yet exist and bucket subdirectory within that directory for the logged in user"""
    s3_sub_path = f"AWS/users/{_username}/s3"
    try:
        os.makedirs(s3_sub_path, exist_ok=False)
    except:
        pass
    bucket_sub_path = f"AWS/users/{_username}/s3/{bucket.name}"
    os.makedirs(bucket_sub_path, exist_ok=True)

def sel_bucket(_username, bucket_name, transfer_func):
    """Allows user to select an existing created bucket"""
    try: #try to query database under username and bucket name combo and load into exiting_bucket instance
        user_id = name_to_id('user_credentials', 'user_id', 'username', _username)
        bucket_id = name_to_id('s3', 'bucket_id', 'name', bucket_name)
        existing_bucket = s3_bucket()
        for attribute in existing_bucket.properties:
            existing_bucket.define_bucket_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM s3 WHERE user_id = {user_id} AND bucket_id = {bucket_id}"), return_result=True)[0])
        #pass username and bucket information to transfer function, which will transfer the user to the bucket specific terminal rather than staying in the s3 general terminal
        transfer_func(_username, existing_bucket)
    except: #if no bucket values are returned by the query, then an error will be thrown and we can inform the user that specified bucket was not found for the logged in user
        print(f'THERE IS NO BUCKET UNDER NAME {bucket_name} FOR USER {_username}!')

def mk_bucket(_username, bucket_name, transfer_func):
    """Creates a new s3 bucket, s3 bucket subdirectory within s3 directory, and persists information in DB"""
    new_bucket = s3_bucket() # create instance of s3_bucket object
    new_bucket = set_vv_abap(new_bucket, 'name') # set values that do not have default values
    new_bucket.define_bucket_properties('arn', f'arn:aws:s3:::{new_bucket.name}')
    ow_def_set = input('Override default settings? (Y/N): ')
    while ow_def_set not in ['Y', 'N']:
        print('ERROR! ENTER Y or N!')
        ow_def_set = input('Override default settings? (Y/N): ')
    if ow_def_set == 'Y': # allow user to override default settings before bucket creation, if desired
        print(f'Avaliable changeable properties: {','.join(new_bucket.properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
        while def_pv_to_change != 'DONE': # if overriding default settings, continue to ask about setting changes until user types 'DONE'
            try:
                new_bucket = set_vv_abap(new_bucket, def_pv_to_change) # allow user to set attribute, if the bucket attribute exists
            except:
                pass # if attribute does not exist and error thrown, simply ignore
            print(f'Avaliable changeable properties: {','.join(new_bucket.properties)}\n')
            def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    upload_properties_to_db(new_bucket, _username)
    create_s3_directory(_username, new_bucket)

def del_bucket_ap(_username, bucket_name, transfer_func):
    """User access point to run function 'remove_bucket_db_dir' which removes a specified bucket and bucket sub directory"""
    try:
        remove_bucket_db_dir(create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)[0]
                            ,create_db_connection(text(f"SELECT bucket_id FROM s3 WHERE name = '{bucket_name}'"), return_result=True)[0])
        shutil.rmtree(f"AWS/users/{_username}/s3/{bucket_name}")
    except:
        print(f'A VALID BUCKET NAME FOR THIS USER MUST BE SPECIFIED! BUCKET DELETE UNSUCCESSFUL!')

def updt_bucket_ap(_username, bucket_name, transfer_func):
    """User access point to run function 'update_bucket' which updates settings for a specified bucket"""
    update_bucket(create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)[0]
                 ,create_db_connection(text(f"SELECT bucket_id FROM s3 WHERE name = '{bucket_name}'"), return_result=True)[0])

def ls_bucket(_username, bucket_name, transfer_func):
    """Lists all active buckets owned by the user"""
    user_id = create_db_connection(text(f"SELECT user_id from user_credentials WHERE username = '{_username}'"), return_result=True)[0]
    for bucket_name in create_db_connection(text(f"SELECT name FROM s3 WHERE user_id = {user_id}"), return_result=True):
        print(f'{bucket_name}')

def bucketSettings(_username, bucket_name, transfer_func):
    """Displays all settings/properties for a s3_bucket instance"""
    dummy_bucket = s3_bucket()
    for property in dummy_bucket.properties:
        print(f'{property}')

if __name__ == '__main__':
    pass
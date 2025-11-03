from datetime import datetime
import logging
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
from UI.app_logging import create_log_entry
from helper_scripts.utils import prompt_validation
import random
import os
import pytz
import shutil
from helper_scripts.sql_helper import create_row, create_db_connection, name_to_id, row_action, postgres_format
from helper_scripts.hadoop_helper import delete_hdfs_file, upload_hdfs_file, upload_pa_table, read_hdfs_file
from cortex.cortex import sel_node

class cortex_file:
    """cortex file object attributes for files within a node currently include:
    
    properties: a list of properties for this object
    uri: the uniform resource identifier for the file
    nrn: NeuronXY resource name for the file
    version: file version (will be 0 if versioning is disabled on the node)
    etag: unique 32 character identifier for the file
    file_url: file path (In actual NeuronXY, this would be a publicly searchable URL)
    owner: owner of the file in the node
    size: the size of the file, in bytes, KB, MG, or GB
    type: file type
    storage_class: storage class for the file
    tags: list of tags associated with the node
    """
    def __init__(self,
                properties=['uri', 'nrn', 'sub_version_id', 'version_id', 'etag', 'file_url', 'owner', 'creation_date', 'last_modified', 'size', 'type', 'storage_class', 'tags'],
                uri='', 
                nrn='',
                sub_version_id=0,
                version_id='', 
                etag='', 
                file_url='', 
                owner='',
                creation_date='',
                last_modified='', 
                size='', 
                type='', 
                storatge_class='standard',
                tags=''):
        self.properties = properties
        self.uri = uri
        self.nrn = nrn
        self.sub_version_id = sub_version_id
        self.version_id = version_id
        self.etag = etag
        self.file_url = file_url
        self.owner = owner
        self.creation_date = creation_date
        self.last_modified = last_modified
        self.size = size
        self.type = type
        self.storage_class = storatge_class
        self.tags = tags.split(',')
        
    def get_file_properties(self, attr_name):
        """returns the current node property value for a given attribute"""
        if hasattr(self, attr_name):  # Ensure the attribute exists
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")

    def define_file_properties(self, attr_name, value):
        if hasattr(self, attr_name):  # Ensure the attribute exists
            setattr(self, attr_name, value)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")

    def set_file_properties(self, attr):
        """Allows overrding of default file properties (only used for storage class for now)"""
        if attr == 'storage_class':
            storage_class = prompt_validation('Please enter a valid storage class for this file: ', req_vals=['standard', 'intelligent-tiering', 'standard-IA', 'one-zone-IA', 'glacier-instant-retrieval', 'glacier-flexible-retrieval', 'glacier-deep-archive'], prnt_req_vals=True)
            self.storage_class = storage_class
        if attr == 'tags':
            tags = input('Please enter tags you want associated with the file, seperated by commas: ').split(',')
            if '' in tags:
                tags.remove('')
            self.tags = tags

def version_node(_username, node, file_name):
    """Will version an file depending on the ordinal value of the file upload"""
    if node.node_versioning:
        obj_sub_vers_ids = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'nrn'], 
                                                        [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                         name_to_id('cortex', 'node_id', 'name', node.name),
                                                         f"'nrn:neuron:cortex:::{node.name}/{file_name}'"], 'SELECT sub_version_id'), return_result = True)
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
    """Gets size of file in bytes, KB, MB, or GB depending on which measure is the most practical for the given file"""
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

def persist_file(_node, username, file_name, loacl_file_path, hdfs_file_path, non_replica=False, bypass_input=False, synapse_run=False):
    """Creates an instance of cortex_file object, loads data according to the file uploaded, then after defining all
    file attributes, uploads those attributes to the DB"""
    cols = ['user_id', 'node_id']
    user_id = name_to_id('user_credentials', 'user_id', 'username', username) #retrieve user_id from DB based off username
    node_id = name_to_id('cortex', 'node_id', 'name', _node.name) #retrieve node_id from DB based off node name
    prepro_vals = [user_id, node_id]
    version_vals = version_node(username, _node, file_name)
    new_file = cortex_file( #instaniate cortex_file and define values based on file information
        uri=f'cortex://{_node.name}/{file_name}',
        nrn=f'nrn:neuron:cortex:::{_node.name}/{file_name}',
        sub_version_id = version_vals[0],
        version_id = version_vals[1],
        etag= etag_create(),
        file_url = f'{hdfs_file_path}',
        owner = username,
        creation_date = datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None),
        last_modified = datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None),
        size = get_file_size_in_units(f'{loacl_file_path}'),
        type = file_name.split('.')[1])
    if not non_replica or not bypass_input:
        override_defs = prompt_validation('Override default settings? (Y/N): ', req_vals=['Y', 'N'], bool_eval={'Y': True, 'N': False}, bp_input=[bypass_input, 'N'])
        if override_defs:
            new_file = override_defaults(new_file)
    for attribute in new_file.properties:
        cols.append(attribute) #columns to update in DB, matched with file attributes
        prepro_vals.append(new_file.get_file_properties(attribute)) #values to uploaded, pre postgres friendly translation
    # go through the unprocessed values, and change formating to postgres friendly as per value data type
    vals = postgres_format(prepro_vals)
    create_db_connection(create_row('cortex_node', cols, vals))
    create_log_entry(username, 'POST', 'uploadFile', 'cortex', 'node', _node, 'file', new_file, synapse_run)

def file_replication(_username, _node, _file_name, _perm_tag):
    """Replicates any files that have the target from node specified as the node that got the file uploaded to it"""
    replicate_upload_to_bnm = create_db_connection(row_action('cortex', ['replication_node_id'], [name_to_id('cortex', 'node_id', 'name', _node.name)], action_type='SELECT name'), return_result=True)
    for bname in replicate_upload_to_bnm:
        node_rep_to = sel_node(_username, bname, None, True)
        upload_file(_username, node_rep_to, _file_name, None, True)

def override_defaults(file):
    """Enters loop to override default values upon file instantiation"""
    updateable_properties = ['storage_class', 'tags']
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change in updateable_properties:
            file.set_file_properties(def_pv_to_change)
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    return file

def update_file(_username, _node, _file_name, _perm_tag):
    """allows for users to update existing cortex file settings (provided that they are overridable)"""
    existing_file = cortex_file() #instantiate cortex_node instance
    usr_buk_obj_ids = [name_to_id('user_credentials', 'user_id', 'username', _username), name_to_id('cortex', 'node_id', 'name', _node.name), name_to_id('cortex_node', 'file_id', 'nrn', f'nrn:neuron:cortex:::{_node.name}/{_file_name}')]
    for attribute in existing_file.properties: #pull node values from database and set cortex_node attributes on exiting_node instance
        existing_file.define_file_properties(attribute, create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'file_id'], usr_buk_obj_ids, action_type=f'SELECT {attribute}'), return_result=True)[0])

    # give user updatable properties and prompt for values to change until user types 'DONE'
    existing_file = override_defaults(existing_file)

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update file values
    vals = postgres_format([existing_file.get_node_properties(property) for property in existing_file.properties])
    cols = existing_file.properties
    for i in range(len(vals)):
        create_db_connection(row_action('', ['user_id', 'node_id', 'file_id'], usr_buk_obj_ids, f'UPDATE cortex_node SET {cols[i]} = {vals[i]}', frm_keywrd=''))
        create_log_entry(_username, 'PUT', 'updateFile', 'cortex', 'node', _node, 'file', existing_file)

def upload_file(_username, node, file_name, perm_tag, replicate_process=False, pa_upload=False, pa_table=None, bp_input=False, synapse_run=False):
    """Uploads file from externalFiles folder into an cortex node and records details in the DB"""
    source = f"NeuronXY/externalFiles/{file_name}"
    destination = f"{_username}/cortex/{node.name}/{file_name}"
    try: # attempt to find file name in externalFiles and move file to subdirectory. Afterwards, record file details in DB
        if pa_upload:
            upload_pa_table(pa_table, destination)
        else:
            upload_hdfs_file(source, destination)
        obj_exists = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'nrn'], 
                                                        [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                         name_to_id('cortex', 'node_id', 'name', node.name),
                                                         f"'nrn:neuron:cortex:::{node.name}/{file_name}'"], 'SELECT 1'), return_result = True)
        if ((node.node_versioning and 1 in obj_exists) or obj_exists == []):
            if not replicate_process:
                persist_file(node, _username, file_name, source, destination, True, bp_input, synapse_run=synapse_run)
                file_replication(_username, node, file_name, None)
            else:
                persist_file(node, _username, file_name, source, destination, synapse_run=synapse_run)
        else:
            create_db_connection(row_action('', ['user_id', 'node_id', 'nrn'], 
                                                [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                name_to_id('cortex', 'node_id', 'name', node.name),
                                                f"'nrn:neuron:cortex:::{node.name}/{file_name}'"], f"UPDATE cortex_node SET last_modified = '{datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None)}'", frm_keywrd=''))
    except Exception as e: # if file is not found, throw error that file could not be found
        logging.error(f'An error occured', exc_info=True)
        print(f'ERROR! No file under name "{file_name}" found! Please ensure file is in the "externalFiles" directory or ensure file name is spelled correctly!')

def delete_file(_username, node, file_name, perm_tag:bool=False):
    """Removes file from DB and node directory"""
    source = f"{_username}/cortex/{node.name}/{file_name}"
    if (node.node_versioning and not perm_tag): # if node versioning is enabled and delete is not specified as permenent
        sub_version_ids = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'nrn'], 
                                                            [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                            name_to_id('cortex', 'node_id', 'name', node.name),
                                                            f"'nrn:neuron:cortex:::{node.name}/{file_name}'"], 'SELECT file_id, sub_version_id, version_id', order_state='ORDER BY sub_version_id DESC'), multi_return=[True, 3])
        if sub_version_ids[0] == []: # if no results returned, file does not exist
            print(f'ERROR! No file under name "{file_name}" found! Please ensure file exists in node {node.name} or ensure file name is spelled correctly!')
        elif (sub_version_ids[2][0] == 'delete-marker'): # if there is  delete marker and node is versioned, you must specifiy a permenanet delete for a delete to take place
            print(f'ERROR! Final file version already marked as deleted, please use the --perm tag or disable versioning if you wish to permenantly delete this file!')
        elif sub_version_ids[1][0] == sub_version_ids[1][-1]: # this indicates that there is only the oldest version still avaliable in the DB. Update version to a delete-marker
            create_db_connection(row_action('', ['user_id', 'node_id', 'file_id'], 
                                                [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                name_to_id('cortex', 'node_id', 'name', node.name),
                                                sub_version_ids[0][0]], f"UPDATE cortex_node SET version_id = 'delete-marker'", frm_keywrd=''))
        elif sub_version_ids[1][0] > sub_version_ids[1][-1]: # delete the latest version if there is more than one version of an file
            create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'file_id'], [name_to_id('user_credentials', 'user_id', 'username', _username),
            name_to_id('cortex', 'node_id', 'name', node.name), sub_version_ids[0][0]], 'DELETE'))
    else: # perm delete or delete with node versioning disabled
        try: # attempt to find file name in externalFiles and move file to subdirectory. Afterwards, record file details in DB
            delete_hdfs_file(source)
            create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'nrn'], [name_to_id('user_credentials', 'user_id', 'username', _username),
            name_to_id('cortex', 'node_id', 'name', node.name), f"'nrn:neuron:cortex:::{node.name}/{file_name}'"], 'DELETE'))
        except: # if file is not found, throw error that file could not be found
            print(f'ERROR! No file under name "{file_name}" found! Please ensure file exists in node {node.name} or ensure file name is spelled correctly!')

def get_file(user_id, node_id, file_id, out_file_format, perm_tag):
    """Accesses a file from HDFS and loads into pandas DF for Synapse (and other service) manipulation"""
    file_path = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'file_id'], 
                                                               [user_id, node_id, file_id], 'SELECT DISTINCT file_url, type'), multi_return=[True, 2])
    file_df = read_hdfs_file(file_path[0][0], file_path[1][0], out_file_format)
    return file_df
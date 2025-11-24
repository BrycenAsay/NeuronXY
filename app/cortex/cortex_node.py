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
    hdfs_path: file path
    owner: owner of the file in the node
    size: the size of the file, in bytes, KB, MG, or GB
    file_extension: file extension
    tags: list of tags associated with the node
    """
    def __init__(self,
                properties=['hdfs_path', 'owner', 'creation_date', 'last_modified', 'size', 'file_extension', 'tags'],
                hdfs_path='', 
                owner='',
                creation_date='',
                last_modified='', 
                size='', 
                file_extension='',
                tags=''):
        self.properties = properties

        self.hdfs_path = hdfs_path
        self.owner = owner
        self.creation_date = creation_date
        self.last_modified = last_modified
        self.size = size
        self.file_extension = file_extension
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
        """Allows overrding of default file properties"""
        if attr == 'tags':
            tags = input('Please enter tags you want associated with the file, seperated by commas: ').split(',')
            if '' in tags:
                tags.remove('')
            self.tags = tags

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
    new_file = cortex_file( #instaniate cortex_file and define values based on file information
        hdfs_path = f'{hdfs_file_path}',
        owner = username,
        creation_date = datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None),
        last_modified = datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None),
        size = get_file_size_in_units(f'{loacl_file_path}'),
        file_extension = file_name.split('.')[1])
    if not non_replica and not bypass_input:
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

def override_defaults(file):
    """Enters loop to override default values upon file instantiation"""
    updateable_properties = ['tags']
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change in updateable_properties:
            file.set_file_properties(def_pv_to_change)
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    return file

def update_file(_username, _node, _file_name):
    """allows for users to update existing cortex file settings (provided that they are overridable)"""
    existing_file = cortex_file() #instantiate cortex_node instance
    usr_buk_obj_ids = [name_to_id('user_credentials', 'user_id', 'username', _username), name_to_id('cortex', 'node_id', 'name', _node.name), name_to_id('cortex_node', 'file_id', 'hdfs_path', f'{_node.name}/{_file_name}')]
    for attribute in existing_file.properties: #pull node values from database and set cortex_node attributes on exiting_node instance
        existing_file.define_file_properties(attribute, create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'file_id'], usr_buk_obj_ids, action_type=f'SELECT {attribute}'), return_result=True)[0])

    # give user updatable properties and prompt for values to change until user types 'DONE'
    existing_file = override_defaults(existing_file)

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update file values   
    vals = postgres_format([existing_file.get_file_properties(property) for property in existing_file.properties])
    cols = existing_file.properties
    for i in range(len(vals)):
        create_db_connection(row_action('', ['user_id', 'node_id', 'file_id'], usr_buk_obj_ids, f'UPDATE cortex_node SET {cols[i]} = {vals[i]}', frm_keywrd=''))
    create_log_entry(_username, 'PUT', 'updateFile', 'cortex', 'node', _node, 'file', existing_file)

def upload_file(_username, node, file_name, pa_upload=False, pa_table=None, bp_input=False, synapse_run=False):
    """Uploads file from externalFiles folder into an cortex node and records details in the DB"""
    source = f"NeuronXY/externalFiles/{file_name}"
    destination = f"/cortex/{_username}/{node.name}/{file_name}"
    try: # attempt to find file name in externalFiles and move file to subdirectory. Afterwards, record file details in DB
        if pa_upload:
            upload_pa_table(pa_table, destination, _username)
        else:
            upload_hdfs_file(source, destination, _username)
        obj_exists = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'hdfs_path'], 
                                                        [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                         name_to_id('cortex', 'node_id', 'name', node.name),
                                                         f"'/cortex/{_username}/{node.name}/{file_name}'"], 'SELECT 1'), return_result = True)
        if obj_exists == []:
            persist_file(node, _username, file_name, source, destination, bypass_input=bp_input, synapse_run=synapse_run)
        else:
            create_db_connection(row_action('', ['user_id', 'node_id', 'hdfs_path'], 
                                                [name_to_id('user_credentials', 'user_id', 'username', _username),
                                                name_to_id('cortex', 'node_id', 'name', node.name),
                                                f"'/cortex/{_username}/{node.name}/{file_name}'"], f"UPDATE cortex_node SET last_modified = '{datetime.now(tz=pytz.timezone('US/Mountain')).replace(tzinfo=None)}'", frm_keywrd=''))

    except Exception as e: # if file is not found, throw error that file could not be found
        logging.error(f'An error occured', exc_info=True)
        print(f'ERROR! No file under name "{file_name}" found! Please ensure file is in the "externalFiles" directory or ensure file name is spelled correctly!')

def delete_file(_username, node, file_name, perm_tag:bool=False):
    """Removes file from DB and node directory"""
    source = f"/cortex/{_username}/{node.name}/{file_name}"
    try: # attempt to find file name in externalFiles and move file to subdirectory. Afterwards, record file details in DB
        delete_hdfs_file(source, _username)
        create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'hdfs_path'], [name_to_id('user_credentials', 'user_id', 'username', _username),
        name_to_id('cortex', 'node_id', 'name', node.name), f"'/cortex/{_username}/{node.name}/{file_name}'"], 'DELETE'))
    except: # if file is not found, throw error that file could not be found
        print(f'ERROR! No file under name "{file_name}" found! Please ensure file exists in node {node.name} or ensure file name is spelled correctly!')

def get_file(user_id, node_id, file_id, out_file_format):
    """Accesses a file from HDFS and loads into pandas DF for Synapse (and other service) manipulation"""
    file_path = create_db_connection(row_action('cortex_node', ['user_id', 'node_id', 'file_id'], 
                                                               [user_id, node_id, file_id], 'SELECT DISTINCT hdfs_path, file_extension'), multi_return=[True, 2])
    file_df = read_hdfs_file(file_path[0][0], name_to_id('user_credentials', 'user_id', 'username', user_id, reversed=True), file_path[1][0], out_file_format)
    return file_df
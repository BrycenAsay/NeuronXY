from helper_scripts.sql_helper import create_row, create_db_connection, row_action, update_row_dos_id, name_to_id, postgres_format
from helper_scripts.hadoop_helper import create_hdfs_directory, delete_hdfs_directory, read_hdfs_file
from helper_scripts.utils import prompt_validation, init_object
from sqlalchemy import text
from UI.app_logging import create_log_entry
import logging
import traceback

class cortex_node:
    """cortex node object, attributes for a node currently include:
    
    properties: a list of properties for this object
    name: the name for the node
    tags: list of tags associated with the node
    """
    def __init__(self,
                 properties = ['name', 'nrn', 'tags'],
                 name = '',
                 nrn = '',
                 tags=[]):
        self.properties = properties
        self.name = name
        self.nrn = nrn
        self.tags = tags

    def set_node_properties(self, attr):
        """Allows a terminal user to set node properties by passing the attribute string as the attr argument"""
        if attr == 'name':
            self.name = prompt_validation('Please enter a name for this node between 3 to 63 characters> ', req_len_range=[3, 63])
        if attr == 'tags':
            tags = input('Please enter tags you want associated with the node, seperated by commas> ').split(',')
            if '' in tags:
                tags.remove('')
            self.tags = tags

    def define_node_properties(self, attr_name, value):
        if hasattr(self, attr_name):  # Ensure the attribute exists
            setattr(self, attr_name, value)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")
        
    def get_node_properties(self, attr_name):
        """returns the current node property value for a given attribute"""
        if hasattr(self, attr_name):  # Ensure the attribute exists
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")

def upload_properties_to_db(node, _username):
    """Uploads node properties to the DB"""
    cols = ['user_id'] + node.properties
    prepro_vals = create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)
    vals = []
    for property in node.properties:
        prepro_vals.append(node.get_node_properties(property))
    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query
    vals = postgres_format(prepro_vals)
    create_db_connection(create_row('cortex', cols, vals))

def remove_node_db_dir(_user_id, _node_id):
    """deletes both the node row in the DB and removes the cortex node subdirectory"""
    create_db_connection(row_action('cortex', ['user_id', 'node_id'], [_user_id, _node_id], 'DELETE'))

def update_node(_user_id, _node_id):
    """allows for users to update existing cortex node settings"""
    existing_node = cortex_node() #instantiate cortex_node instance
    for attribute in existing_node.properties: #pull node values from database and set cortex_node attributes on exiting_node instance
        existing_node.define_node_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM cortex WHERE user_id = {_user_id} AND node_id = {_node_id}"), return_result=True)[0])
    
    # give user updatable properties (removed name, nrn, and replication_node_id, name and nrn are immutable after the node gets created and replication_node_id must go through a seperate validation process) and prompt for values to change until user types 'DONE'
    updateable_properties = [x for x in existing_node.properties]
    updateable_properties.remove('name')
    updateable_properties.remove('nrn')
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change in updateable_properties:
            existing_node.set_node_properties(def_pv_to_change)
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update node values
    vals = postgres_format([existing_node.get_node_properties(property) for property in existing_node.properties])
    cols = existing_node.properties
    for i in range(len(vals)):
        create_db_connection(row_action('', ['user_id', 'node_id'], [_user_id, _node_id], action_type=f'UPDATE cortex SET {cols[i]} = {vals[i]}', frm_keywrd=''))

def create_cortex_directory(_username, node):
    """Creates an cortex directory if it does not yet exist and node subdirectory within that directory for the logged in user"""
    cortex_path = f"cortex/{_username}/{node.name}"
    try:
        create_hdfs_directory(cortex_path, _username)
    except Exception as e:
        print(f'{e}')

def sel_node(_username, node_name, transfer_func, override_transfer=False):
    """Allows user to select an existing created node"""
    try: #try to query database under username and node name combo and load into exiting_node instance
        user_id = name_to_id('user_credentials', 'user_id', 'username', _username)
        node_id = name_to_id('cortex', 'node_id', 'name', node_name)
        existing_node = cortex_node()
        for attribute in existing_node.properties:
            existing_node.define_node_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM cortex WHERE user_id = {user_id} AND node_id = {node_id}"), return_result=True)[0])
        #pass username and node information to transfer function, which will transfer the user to the node specific terminal rather than staying in the cortex general terminal
        if not override_transfer:
            transfer_func(_username, existing_node)
        else:
            return existing_node
    except: #if no node values are returned by the query, then an error will be thrown and we can inform the user that specified node was not found for the logged in user
        print(f'THERE IS NO BUCKET UNDER NAME {node_name} FOR USER {_username}!')

def override_defaults(node):
    """Enters loop to override default values upon object instantiation"""
    changeable_properties = [x for x in node.properties]
    changeable_properties.remove('name')
    print(f'Avaliable changeable properties: {','.join(changeable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE': # if overriding default settings, continue to ask about setting changes until user types 'DONE'
        try:
            changeable_properties.index(def_pv_to_change)
            node.set_node_properties(def_pv_to_change) # allow user to set attribute, if the node attribute exists
        except:
            pass # if attribute does not exist and error thrown, simply ignore
        print(f'Avaliable changeable properties: {','.join(changeable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    return node

def mk_node(_username, node_name, transfer_func, bypass_input=False, **kwargs):
    """Creates a new cortex node, cortex node subdirectory within cortex directory, and persists information in DB"""
    if not bypass_input:
        new_node = cortex_node() # create instance of cortex_node object
        new_node.set_node_properties('name') # set values that do not have default values
        if (1 in create_db_connection(text(f"SELECT 1 FROM cortex WHERE name = '{new_node.get_node_properties('name')}'"), return_result=True)):
            print('ERROR! Node name is already in use by an existing user. Please choose a different node name!')
            return
        ow_def_set = prompt_validation('Override default settings? (Y/N): ', ['Y', 'N'], bp_input=[bypass_input, 'N'])
        if ow_def_set == 'Y': # allow user to override default settings before node creation, if desired
            new_node = override_defaults(new_node)
    else:
        new_node = init_object(cortex_node(), **kwargs)
    new_node.define_node_properties('nrn', f'nrn:neuron:cortex:::{new_node.name}')
    upload_properties_to_db(new_node, _username)
    create_cortex_directory(_username, new_node)
    create_log_entry(_username, 'POST', 'cortexNodeAdd', 'cortex', None, None, 'node', new_node)

def del_node_ap(_username, node_name, transfer_func):
    """User access point to run function 'remove_node_db_dir' which removes a specified node and node sub directory"""
    try:
        create_log_entry(_username, 'DELETE', 'nodeDelete', 'cortex', None, None, 'node', sel_node(_username, node_name, None, override_transfer=True))
        remove_node_db_dir(create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)[0]
                            ,create_db_connection(text(f"SELECT node_id FROM cortex WHERE name = '{node_name}'"), return_result=True)[0])
        delete_hdfs_directory(f"/cortex/{_username}/{node_name}", _username)
    except Exception as e:
        logging.error(e)
        traceback.print_exc()

def updt_node_ap(_username, node_name, transfer_func):
    """User access point to run function 'update_node' which updates settings for a specified node"""
    create_log_entry(_username, 'PUT', 'nodeUpdate', 'cortex', None, None, 'node', sel_node(_username, node_name, None, override_transfer=True))
    update_node(create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)[0]
                 ,create_db_connection(text(f"SELECT node_id FROM cortex WHERE name = '{node_name}'"), return_result=True)[0])

def ls_node(_username, node_name, transfer_func, exclude_nodes:list=[], return_list=False):
    """Lists all active nodes owned by the user"""
    node_list = []
    user_id = create_db_connection(text(f"SELECT user_id from user_credentials WHERE username = '{_username}'"), return_result=True)[0]
    if return_list:
        nn_list = [x for x in create_db_connection(text(f"SELECT name FROM cortex WHERE user_id = {user_id}"), return_result=True)]
        for nn in exclude_nodes:
            nn_list.remove(nn)
        return nn_list
    for node_name in create_db_connection(text(f"SELECT name FROM cortex WHERE user_id = {user_id}"), return_result=True):
        if node_name not in exclude_nodes:
            node_list.append(node_name)
            print(f'{node_name}')

def nodeSettings(_username, node_name, transfer_func):
    """Displays all settings/properties for a cortex_node instance"""
    dummy_node = cortex_node()
    for property in dummy_node.properties:
        print(f'{property}')

def node_dump(user_id, node_id):
    """Dumps node contents into a list of pandas dictionaries (these dfs are NOT indexed)"""
    file_paths = create_db_connection(row_action('cortex_node', ['user_id', 'node_id'], 
                                                [user_id, node_id], 'SELECT DISTINCT hdfs_path, file_extension'), multi_return=[True, 2])
    for i in range(len(file_paths[0])):
        file_df = read_hdfs_file(file_paths[0][i], name_to_id('user_credentials', 'user_id', 'username', user_id, reversed=True), file_paths[1][i])
    return file_df
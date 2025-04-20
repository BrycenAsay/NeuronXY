import json
import shutil
import logging
import os
from helper_scripts.sql_helper import create_row, create_db_connection, row_action, update_row_dos_id, name_to_id
from helper_scripts.hadoop_helper import create_hdfs_direcotry, delete_hdfs_direcotry, read_hdfs_file
from sqlalchemy import text

y_or_n_input = lambda x: True if x == 'Y' else False # returns true if value is 'Y' (yes) otherwise defaults to False

class cortex_node:
    """cortex node object, attributes for a node currently include:
    
    properties: a list of properties for this object
    name: the name for the node
    block_public_access: dictates wether the node is or is not publicly accessible
    acl_enabled: dicates wether or not access control lists are endabled
    node_policy: dictates permissions for specific cortex node actions
    node_type: wether node is general purpose or directory
    node_versioning: dictates wether or not file versions are kept track of
    tags: list of tags associated with the node
    encrypt_method: dictates the encryption method for files within the node
    node_key: a local node key that that lowers calls to AWS KMS
    file_lock: prevents files from being deleted or overwritten when specified
    """
    def __init__(self,
                 properties = ['name', 'nrn', 'node_type', 'node_versioning', 'acl_enabled', 
                               'block_public_access', 'node_key', 'file_lock', 'encrypt_method',
                               'node_policy', 'tags', 'file_replication', 'replication_node_id'],
                 name = '',
                 nrn = '',
                 node_type = 'gp', 
                 node_versioning:bool=False, 
                 acl_enabled:bool=False, 
                 block_public_access:bool=True, 
                 node_key:bool=True,
                 file_lock:bool=False,
                 encrypt_method='SSE-CORTEX', 
                 node_policy=json.dumps({}), 
                 tags=[],
                 file_replication:bool=False,
                 replication_node_id='Null'):
        self.properties = properties
        self.name = name
        self.nrn = nrn
        self.block_public_access = block_public_access
        self.acl_enabled = acl_enabled
        self.node_policy = node_policy
        self.node_type = node_type
        self.node_versioning = node_versioning
        self.tags = tags
        self.encrypt_method = encrypt_method
        self.node_key = node_key
        self.file_lock = file_lock
        self.file_replication = file_replication
        self.replication_node_id = replication_node_id

    def set_node_properties(self, attr):
        """Allows a terminal user to set node properties by passing the attribute string as the attr argument"""
        if attr == 'name':
            self.name = input('Please enter a name for this node between 3 to 63 characters> ')
        if attr == 'block_public_access':
            self.block_public_access = y_or_n_input(input('Would you like to block public access? (Y/N)> '))
        if attr == 'acl_enabled':
            self.acl_enabled = y_or_n_input(input('Would you like to enable ACLs? (Y/N)> '))
        if attr == 'node_policy':
            try:
                self.node_policy = json.dumps(input('Please enter a node policy> '))
            except:
                self.node_policy = 'NOT A VALID JSON OBJECT'
        if attr == 'node_type':
            self.node_type = input('Please enter a valid node type (gp or dir)> ')
        if attr == 'node_versioning':
            self.node_versioning = y_or_n_input(input('Would you like to enable node versioning? (Y/N)> '))
        if attr == 'tags':
            tags = input('Please enter tags you want associated with the node, seperated by commas> ').split(',')
            if '' in tags:
                tags.remove('')
            self.tags = tags
        if attr == 'encrypt_method':
            self.encrypt_method = input('Please enter a valid encryption method (SSE-CORTEX, SEE-KMS, DSSE-KMS)> ')
        if attr == 'node_key':
            self.node_key = y_or_n_input(input('Would you like to create a node key? (Y/N)> '))
        if attr == 'file_lock':
            self.file_lock = y_or_n_input(input('Would you like to enable file lock? (Y/N)> '))
        if attr == 'file_replication':
            self.file_replication = y_or_n_input(input('Would you like to enable file replication? (Y/N)> '))

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

    @staticmethod
    def validate_value(value, object_attr):
        """Validates that the value being set for an attribute is valid where validation is needed, returns
        False where the validation criteria is not met"""
        if object_attr == 'node_type':
            if value not in ['gp', 'dir']:
                print('ERROR! VALUE MUST EITHER BE GP (GENERAL PURPOSE) OR DIR (DIRECTORY)')
                return False
            else:
                return True
        if object_attr == 'encrypt_method':
            if value not in ['SSE-CORTEX', 'SEE-KMS', 'DSSE-KMS']:
                print('ERROR! VALID ENCRYPTION SETTING NOT SELECTED! PLEASE SELECT A VALID ENCRYPTION SETTING!')
                return False
            else:
                return True
        if object_attr == 'name':
            if (3 <= len(value) <= 63) is False:
                print('ERROR! NAME MUST BE BETWEEN 3 AND 63 CHARACTERS!')
                return False
            elif (1 in create_db_connection(text(f"SELECT 1 FROM cortex WHERE name = '{value}'"), return_result=True)):
                print('ERROR! Node name is already in use by an existing user. Please choose a different node name!')
                return False
            else:
                return True
        if object_attr == 'node_policy':
            try:
                parsed_data = json.loads(value)
                return True
            except:
                print('ERROR! VALUE IS NOT IN VALID JSON FORMAT!')
                return False
        else:
            return True

def set_vv_abap(node, _attr):
    """Sets and validates the attribute values for a node instance"""
    node.set_node_properties(_attr)
    while node.validate_value(node.get_node_properties(_attr), _attr) != True:
        node.set_node_properties(_attr)
    return node

def upload_properties_to_db(node, _username):
    """Uploads node properties to the DB"""
    cols = ['user_id'] + node.properties
    prepro_vals = create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)
    vals = []
    for property in node.properties:
        prepro_vals.append(node.get_node_properties(property))
    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query
    for val in prepro_vals:
        if isinstance(val, str):
            if val != 'Null':
                vals.append(f"'{str(val)}'")
            else:
                vals.append(val)
        elif isinstance(val, list):
            vals.append(f"ARRAY{val}::TEXT[]")
        else:
            vals.append(str(val))
    create_db_connection(create_row('cortex', cols, vals))

def remove_node_db_dir(_user_id, _node_id):
    """deletes both the node row in the DB and removes the cortex node subdirectory"""
    create_db_connection(row_action('cortex', ['user_id', 'node_id'], [_user_id, _node_id], 'DELETE'))

def update_node(_user_id, _node_id):
    """allows for users to update existing cortex node settings"""
    existing_node = cortex_node() #instantiate cortex_node instance
    for attribute in existing_node.properties: #pull node values from database and set cortex_node attributes on exiting_node instance
        existing_node.define_node_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM cortex WHERE user_id = {_user_id} AND node_id = {_node_id}"), return_result=True)[0])
    
    # give user updatable properties (removed name and replication_node_id, name is immutable after the node gets created and replication_node_id must go through a seperate validation process) and prompt for values to change until user types 'DONE'
    updateable_properties = [x for x in existing_node.properties]
    updateable_properties.remove('name')
    updateable_properties.remove('replication_node_id')
    print(f'Avaliable changeable properties: {','.join(updateable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE':
        if def_pv_to_change == 'file_replication' and existing_node.file_replication:
            print('\nWARNING! Node replication is already enabled on this node! If you wish to update the target node for file replication, please disable and renable file replication. You can then change the file replication target node!\n')
        if def_pv_to_change in updateable_properties:
            existing_node = set_vv_abap(existing_node, def_pv_to_change)
        print(f'Avaliable updateable properties: {','.join(updateable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    if (existing_node.replication_node_id is None or existing_node.replication_node_id is not None and not existing_node.file_replication):
        existing_node = node_replication(create_db_connection(text(f'SELECT username FROM user_credentials WHERE user_id = {_user_id}'), return_result=True)[0], existing_node, None)

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update node values
    for property in existing_node.properties:
        val = existing_node.get_node_properties(property)
        if isinstance(val, (str, dict)):
            if val != 'Null':
                val = f"'{str(val)}'"
            else:
                val = val
        elif isinstance(val, list):
            val = f"ARRAY{val}::TEXT[]"
        else:
            val = str(val)
        create_db_connection(update_row_dos_id('cortex', property, val, 'user_id', _user_id, 'node_id', _node_id))

def create_cortex_directory(_username, node):
    """Creates an cortex directory if it does not yet exist and node subdirectory within that directory for the logged in user"""
    cortex_path = f"{_username}/cortex/{node.name}"
    try:
        create_hdfs_direcotry(cortex_path)
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
    changeable_properties.remove('replication_node_id')
    print(f'Avaliable changeable properties: {','.join(changeable_properties)}\n')
    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    while def_pv_to_change != 'DONE': # if overriding default settings, continue to ask about setting changes until user types 'DONE'
        try:
            changeable_properties.index(def_pv_to_change)
            node = set_vv_abap(node, def_pv_to_change) # allow user to set attribute, if the node attribute exists
        except:
            pass # if attribute does not exist and error thrown, simply ignore
        print(f'Avaliable changeable properties: {','.join(changeable_properties)}\n')
        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
    return node

def mk_node(_username, node_name, transfer_func):
    """Creates a new cortex node, cortex node subdirectory within cortex directory, and persists information in DB"""
    new_node = cortex_node() # create instance of cortex_node object
    new_node = set_vv_abap(new_node, 'name') # set values that do not have default values
    new_node.define_node_properties('nrn', f'nrn:aws:cortex:::{new_node.name}')
    ow_def_set = input('Override default settings? (Y/N): ')
    while ow_def_set not in ['Y', 'N']:
        print('ERROR! ENTER Y or N!')
        ow_def_set = input('Override default settings? (Y/N): ')
    if ow_def_set == 'Y': # allow user to override default settings before node creation, if desired
        new_node = override_defaults(new_node)
    new_node = node_replication(_username, new_node, None)
    upload_properties_to_db(new_node, _username)
    create_cortex_directory(_username, new_node)

def del_node_ap(_username, node_name, transfer_func):
    """User access point to run function 'remove_node_db_dir' which removes a specified node and node sub directory"""
    try:
        remove_node_db_dir(create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)[0]
                            ,create_db_connection(text(f"SELECT node_id FROM cortex WHERE name = '{node_name}'"), return_result=True)[0])
        delete_hdfs_direcotry(f"{_username}/cortex/{node_name}")
    except:
        print(f'A VALID BUCKET NAME FOR THIS USER MUST BE SPECIFIED! BUCKET DELETE UNSUCCESSFUL!')

def updt_node_ap(_username, node_name, transfer_func):
    """User access point to run function 'update_node' which updates settings for a specified node"""
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

def node_replication(_username, node, transfer_func):
    """If node replication is enabled, we must ensure that a valid replication node is selected, and that there is such a node to select"""
    if node.file_replication and node.replication_node_id not in [None, 'Null']:
        pass
    elif node.file_replication:
        if create_db_connection(row_action('cortex', ['user_id', 'name', 'name'], [name_to_id('user_credentials', 'user_id', 'username', _username), name_to_id('cortex', 'replication_node_id', 'name', name_to_id('cortex', 'node_id', 'name', node.name), 
                                                                                                                                                            reversed=True, one_result=False), f"'{node.name}'"], 
                                                                                                                                                            'SELECT COUNT(*)', not_eq=[False, True, True], group_state='GROUP BY user_id'), return_result=True) in [[0], []]:
            print('ERROR! You must have more than one node to enable file replication on this node! This node will either be created with file replication disabled, or will not have this option updated!')
            node.define_node_properties('file_replication', False)
            node.define_node_properties('replication_node_id', 'Null')
        else:
            ls_node_limit = [node.name] +  name_to_id('cortex', 'replication_node_id', 'name', name_to_id('cortex', 'node_id', 'name', node.name), reversed=True, one_result=False)
            ls_node(_username, node.name, transfer_func, ls_node_limit)
            rep_targ_buck = input('Please enter a target node for file replication into this node. Please note that this prompt will not quit until a valid node_name is specified: ')
            while (rep_targ_buck not in [node_name for node_name in create_db_connection(row_action('cortex', ['user_id', 'name', 'node_id'], [name_to_id('user_credentials', 'user_id', 'username', _username), 
            f"'{node.name}'", name_to_id('cortex', 'replication_node_id', 'name', node.name)], action_type='SELECT name', not_eq=[False, True, True]), return_result=True)] or rep_targ_buck in ls_node_limit):
                ls_node(_username, node.name, transfer_func, ls_node_limit)
                rep_targ_buck = input('Please enter a target node for file replication. Please note that this prompt will not quit until a valid node_name is specified: ')
            node.define_node_properties('replication_node_id', create_db_connection(row_action('cortex', ['user_id', 'name'], [name_to_id('user_credentials', 'user_id', 'username', _username), 
                                                                                                                f"'{rep_targ_buck}'"], 'SELECT node_id'), return_result=True)[0])
    else:
        node.define_node_properties('file_replication', False)
        node.define_node_properties('replication_node_id', 'Null')
    return node

def lifecycle_rules(_username, node_name, transfer_func):
    usr_id = str(name_to_id('user_credentials', 'user_id', 'username', _username))
    buk_id = str(name_to_id('cortex', 'node_id', 'name', node_name))
    if buk_id == '999999999':
        print(f'ERROR! Node {node_name} was not found for user {_username}! Please ensure you sepcify a valid node to add a lifecycle rule to!')
        return
    transition_to = []
    days_till_transition = []
    storage_classes = ['standard-IA', 'intelligent-tiering', 'one-zone-IA', 'glacier-instant-retrieval', 'glacier-flexible-retrieval', 'glacier-deep-archive']
    create_add_to_rule = input('Would you like to create a lifecycle rule or add an additional transition rule? (Y/N): ')
    while create_add_to_rule != 'N':
        print(', '.join(storage_classes))
        trans_class = input('Please enter a class to transition to, please note this will not exit until a class is specified: ')
        while trans_class not in storage_classes:
            print(', '.join(storage_classes))
            trans_class = input('Please enter a class to transition to, please note this will not exit until a class is specified: ')
        if trans_class == 'one-zone-IA':
            storage_classes = storage_classes[storage_classes.index('glacier-instant-retrieval') + 1:len(storage_classes)]
        else:
            storage_classes = storage_classes[storage_classes.index(trans_class) + 1:len(storage_classes)]
        
        if transition_to == [] and trans_class in ['standard-IA', 'one-zone-IA']:
            user_prompt = 'Please enter number of days until the transition takes place, please note it must be 30 days or greater: '
            min_days = 29
        elif transition_to == [] and trans_class not in ['standard-IA', 'one-zone-IA']:
            user_prompt = 'Please enter number of days until the transition takes place, please note it must 1 day or greater: '
            min_days = 0
        elif transition_to[-1] in ['standard-IA', 'one-zone-IA']:
            user_prompt = f'Please enter number of days until the transition takes place, please note it must 30 days or greater than than the transition to {transition_to[-1]} which was {str(days_till_transition[-1])} days: '
            min_days = days_till_transition[-1] + 29
        else:
            user_prompt = f'Please enter number of days until the transition takes place, please note it must 1 day or greater than than the transition to {transition_to[-1]} which was {str(days_till_transition[-1])} days: '
            min_days = days_till_transition[-1]

        days_till_trans = input(user_prompt)
        try:
            days_till_trans = int(days_till_trans)
        except:
            days_till_trans = -1
        while days_till_trans <= min_days:
            days_till_trans = input(user_prompt)
            try:
                days_till_trans = int(days_till_trans)
            except:
                days_till_trans = -1
        
        transition_to.append(trans_class)
        days_till_transition.append(days_till_trans)
        if 'glacier-deep-archive' in transition_to:
            create_add_to_rule = 'N'
        else:
            create_add_to_rule = input('Would you like to create a lifecycle rule or add an additional transition rule? (Y/N): ')
    db_lifecycle_rule_def(usr_id, buk_id, transition_to, days_till_transition)

def db_lifecycle_rule_def(user_id, node_id, trans_to, dt_trans):
    create_db_connection(create_row('lifecycle_rule', ['user_id', 'node_id', 'rule_enabled'], [user_id, node_id, 'True']))
    generated_lfcyc_id = str(create_db_connection(row_action('lifecycle_rule', ['user_id', 'node_id'], [user_id, node_id], 'SELECT lifecycle_id'), return_result=True)[0])
    create_db_connection(row_action('', ['user_id', 'node_id'], [user_id, node_id], f'UPDATE cortex SET lifecycle_id = {generated_lfcyc_id}', frm_keywrd=''))
    trans_to = [f"'{x}'" for x in trans_to]
    dt_trans = [str(x) for x in dt_trans]
    for i in range(len(trans_to)):
        create_db_connection(create_row('lifecycle_transition', ['lifecycle_id', 'transition_to', 'days_till_transition'], [generated_lfcyc_id, trans_to[i], dt_trans[i]]))

def node_dump(user_id, node_id):
    """Dumps node contents into a list of pandas dictionaries (these dfs are NOT indexed)"""
    file_paths = create_db_connection(row_action('cortex_node', ['user_id', 'node_id'], 
                                                [user_id, node_id], 'SELECT DISTINCT file_url, type'), multi_return=[True, 2])
    for i in range(len(file_paths[0])):
        file_df = read_hdfs_file(file_paths[0][i], file_paths[1][i])
    return file_df
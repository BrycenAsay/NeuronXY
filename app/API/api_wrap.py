from UI.account_creation import encrypt_password, salt_pass, persist_creds, create_log_entry, update_creds, check_if_user_exists
from cortex.cortex import cortex_node, persist_node, del_node_ap
from helper_scripts.sql_helper import create_db_connection, row_action, text, name_to_id, postgres_format
from helper_scripts.utils import create_object_json
import jwt
from config import API_SECRET_KEY
import datetime
import jwt
import shutil
import logging

"""API wrappers for account level endpoints"""

def create_creds(creds_config):
    """Creates a new user with defined username and password comhbination. 'unsued_param' exists since each function
    called by the terminal object in terminal.py must have an equivalent number of function parameters so that a parameter
    error is not thrown"""
    username = creds_config['username']
    password = creds_config['password']
    username_exists = create_db_connection(check_if_user_exists('user_credentials', username), return_result=True)
    if 1 in username_exists: # if result returned from select statement, username exists and a different one must be selected
        raise ValueError(f'ERROR! Username {username} is already taken, please choose a different username!')
    password = encrypt_password(salt_pass(username, password))
    persist_creds(username, password) # after getting password and username, persist credentials in the DB

def user_login(creds_config):
    """Verify user credentials and if valid passes that to api.py which then creates and provides the bearer token"""
    username = creds_config['username']
    password = creds_config['password']
    salt = creds_config['salt']
    password = encrypt_password(salt_pass(username, password, salt)) #encrypt password according to salt value provided
    result = create_db_connection(text(f"SELECT 1 FROM user_credentials WHERE username = '{username}' AND password = '{password}'"), return_result=True)
    if 1 in result: # if 1 is returned, we know that the combination exists, login successful, return True and username for acct terminal entry
        token = jwt.encode({'sub': username, 'exp': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)}, key=API_SECRET_KEY, algorithm="HS256")
        return {'access_token': token, 'token_type': 'bearer'}
    else: # otherwise, return False and login not successful
        raise ValueError('EITHER YOUR USERNAME OR PASSWORD WAS NOT VALID MATE!')
    
def get_user(username):
    user_id = name_to_id('user_credentials', 'user_id', 'username', username)
    if user_id == 999999999:
        raise ValueError(f'NO USERNAME UNDER {username} FOUND IN THE SYSTEM! PLEASE CHECK THE SPELLING OR TRY A DIFFERENT USERNAME!')
    return {'username': username, 'user_id': user_id}

def delete_user(username, user_id):
    """Verifies a combination of username, password, and secret salt string credentials and secret salt instruction string
    exist in the database, then deletes the user"""
    if name_to_id('user_credentials', 'user_id', 'username', username) != user_id:
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    create_log_entry(username, 'DELETE', 'userDelete', object_name='user')
    create_db_connection(row_action('user_credentials', ['user_id'], [user_id], 'DELETE'))
    shutil.rmtree(f"NeuronXY/users/{username}")
    print(f'User {username} and any related resources have successfully been deleted')

def change_password(creds_config):
    """Accepts new password, then updates credentials and secretSalt file accordingly"""
    username = creds_config['username']
    user_id = creds_config['user_id']
    new_password = creds_config['password']
    if name_to_id('user_credentials', 'user_id', 'username', username) != user_id:
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    new_password = encrypt_password(salt_pass(username, new_password)) #resalt password, create new secretSalt file
    update_creds(username, new_password) #update credentials in the DB

"""API wrappers for cortex node endpoints"""

def get_node(_username, user_id, node_details):
    """Allows user to select an existing created node"""
    try: #try to query database under username and node name combo and load into exiting_node instance
        if name_to_id('user_credentials', 'user_id', 'username', _username) != user_id:
            raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
        node_name = node_details['name']
        node_id = name_to_id('cortex', 'node_id', 'name', node_name)
        existing_node = cortex_node()
        for attribute in existing_node.properties:
            existing_node.define_node_properties(attribute, create_db_connection(text(f"SELECT {attribute} FROM cortex WHERE user_id = {user_id} AND node_id = {node_id}"), return_result=True)[0])
        return {'node_id': node_id, **create_object_json(existing_node, py_dict=True)}
    except: #if no node values are returned by the query, then an error will be thrown and we can inform the user that specified node was not found for the logged in user
        raise ValueError(f'Bucket name {node_name} not found for user {_username}!')
    
def list_nodes(user_id, username):
    """Lists all active nodes owned by the user"""
    if name_to_id('user_credentials', 'user_id', 'username', username) != user_id:
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    nn_list = [x for x in create_db_connection(text(f"SELECT name FROM cortex WHERE user_id = {user_id}"), return_result=True)]
    return {'nodes': nn_list}

def mk_node(_username, user_id, node_details):
    """Creates a new cortex node, cortex node subdirectory within cortex directory, and persists information in DB"""
    if name_to_id('user_credentials', 'user_id', 'username', _username) != user_id:
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    new_node = cortex_node() # create instance of cortex_node object
    for detail in node_details:
        if node_details[detail] is not None:
            new_node.define_node_properties(detail, node_details[detail])
    if (1 in create_db_connection(text(f"SELECT 1 FROM cortex WHERE name = '{new_node.get_node_properties('name')}'"), return_result=True)):
        raise ValueError('ERROR! Node name is already in use by an existing user. Please choose a different node name!')
    new_node.define_node_properties('nrn', f'nrn:neuron:cortex:::{new_node.name}')
    try:
        persist_node(_username, new_node, user_id)
    except Exception as e:
        logging.Error(e)

def update_node(user_id, username, node_id, updates):
    """Allows for users to update existing cortex node settings"""
    if (name_to_id('user_credentials', 'user_id', 'username', username) != user_id or name_to_id('cortex', 'user_id', 'node_id', node_id) != user_id):
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}/node id {node_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    existing_node_prop = get_node(username, user_id, {'name': name_to_id('cortex', 'node_id', 'name', node_id, reversed=True)}) #instantiate cortex_node instance
    del existing_node_prop['node_id']
    existing_node = cortex_node()
    for key in updates.keys():
        if updates[key] != None:
            existing_node_prop[key] = updates[key]
    for attr in existing_node_prop.keys(): #pull node values from database and set cortex_node attributes on exiting_node instance
        existing_node.define_node_properties(attr, existing_node_prop[attr])

    #ensure values are in Postgres friendly formating based on data type before being written to the SQL query, then create and run update statement to update node values
    vals = postgres_format([existing_node.get_node_properties(property) for property in existing_node.properties])
    cols = existing_node.properties
    for i in range(len(vals)):
        create_db_connection(row_action('', ['user_id', 'node_id'], [user_id, node_id], action_type=f'UPDATE cortex SET {cols[i]} = {vals[i]}', frm_keywrd=''))

def delete_node(user_id, _username, node_id):
    """Deletes a user node"""
    node_name = name_to_id('cortex', 'node_id', 'name', node_id, reversed=True)
    if (name_to_id('user_credentials', 'user_id', 'username', _username) != user_id or name_to_id('cortex', 'user_id', 'node_id', node_id) != user_id):
        raise ValueError(f'Bearer token provided does not authenticate this request for user id {user_id}/node id {node_id}. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
    del_node_ap(_username, node_name, None)
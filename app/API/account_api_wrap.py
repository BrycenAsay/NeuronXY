from UI.account_creation import encrypt_password, salt_pass, persist_creds, create_log_entry, update_creds
from helper_scripts.sql_helper import create_db_connection, row_action, text, name_to_id
import jwt
from config import API_SECRET_KEY
import datetime
from fastapi import Depends, HTTPException
import jwt
import shutil

def create_creds(creds_config):
    """Creates a new user with defined username and password comhbination. 'unsued_param' exists since each function
    called by the terminal object in terminal.py must have an equivalent number of function parameters so that a parameter
    error is not thrown"""
    username = creds_config['username']
    password = creds_config['password']
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
    create_log_entry(username, 'DELETE', 'userDelete', object_name='user')
    create_db_connection(row_action('user_credentials', ['user_id'], [user_id], 'DELETE'))
    shutil.rmtree(f"NeuronXY/users/{username}")
    print(f'User {username} and any related resources have successfully been deleted')

def change_password(creds_config):
    """Accepts new password, then updates credentials and secretSalt file accordingly"""
    try:
        username = creds_config['username']
        user_id = creds_config['user_id']
        new_password = creds_config['password']
        if name_to_id('user_credentials', 'user_id', 'username', username) != user_id:
            raise ValueError('Username and user_id do not match. You may use the get method for the NeuronXY/users endpoint to find the correctly correlating user id')
        new_password = encrypt_password(salt_pass(username, new_password)) #resalt password, create new secretSalt file
        update_creds(username, new_password) #update credentials in the DB
    except Exception as e:
        raise ValueError(e)
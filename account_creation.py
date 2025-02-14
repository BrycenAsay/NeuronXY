import random
import hashlib
import shutil
import getpass
import os
from sqlalchemy import text
from sql_helper import create_row, update_row, check_if_user_exists, create_db_connection, name_to_id, row_action

def persist_creds(_username, _password):
    """Creates database connection and saves password + username combination to the database
    
    Parameters:
    _username: username to insert for the new user
    _password: password to insert for the new user"""
    username_exists = create_db_connection(check_if_user_exists('user_credentials', _username), return_result=True)
    if 1 in username_exists: # if result returned from select statement, username exists and a different one must be selected
        print(f'ERROR! Username {_username} is already taken, please choose a different username!')
        return False
    else: # run 'create row' SQL function to insert details into user_credentials table
        create_db_connection(create_row('user_credentials', ['username', 'password'], [f"'{_username}'", f"'{_password}'"]))

def update_creds(_username, _password):
    """Updates password for a given user
    
    Parameters:
    _username: username of an existing user
    _password: new password for the given user"""
    create_db_connection(update_row('user_credentials', 'password', _password, 'username', _username))

def salt_pass(_username, _password, post_salt='', clear_slt_file=False):
    """Adds four random character strings at random positions into user's specified password for extra security. 
    Encrypts and scrambles salting instructions to make manual salting of preditable passwords more difficult
    Will create a secret_salt.txt file for the user, the written string will need to be used when logging in
    
    Parameters:
    _username: username for the given user
    _password: if post_salt instructions specified, this is the unencrypted salted password, otherwise it's the 
    unencrypted regular password and a salted password along with a salt instruction string will be generated
    post_salt: if passing a salted password, pass the salt instructions
    clear_slt_file: if reseting a password, pass True so that file is cleared and new salt instruction code can be written"""
    salted_pass = []
    if clear_slt_file: #clears contents of secretSalt file if resetting password
        user_sub_path = f"AWS/users/{_username}"
        ss_sub_path = f"AWS/users/{_username}/SecretSalt"
        salt_file_path = os.path.join('AWS', 'users', _username, 'SecretSalt', f'secretSalt_{_username}.txt')
        with open(salt_file_path, 'w') as f:
            pass
    
    if post_salt == '': #code to generate secret salt code
        four_intervals_o_salt = {'at_pos': [], 'salt': [], 'len': [], 'num_confuse_ap': [random.randint(123, 321) for _ in range(4)], 'num_confuse_len': [random.randint(123, 321) for _ in range(4)]}
        for i in range(4):
            four_intervals_o_salt['at_pos'].append(random.randint(1, 4) + (4 * i)) #position to insert chars at
            salt_start = [] #list containing random char combination to create string
            m = random.randint(1, 4) #length of string to insert
            four_intervals_o_salt['len'].append(m) #len of salt strings
            for z in range(m):
                salt_start.append(chr(random.randint(34, 126)))
            four_intervals_o_salt['salt'].append(''.join(salt_start)) # create string from salt_start list and insert at 'salt' key
        # creating the salt instruct code, adding in numbers to encrypt salt lengths as well as salt starting positions
        salt_instruct = []
        for i in range(2):
            salt_instruct.append(str(four_intervals_o_salt['len'][i] + four_intervals_o_salt['num_confuse_len'][i]))
            salt_instruct.append(str(four_intervals_o_salt['at_pos'][i] + four_intervals_o_salt['num_confuse_ap'][i]))
        for i in range(4):
            salt_instruct.append(str(four_intervals_o_salt['salt'][i]))
        for i in range(2, 4):
            salt_instruct.append(str(four_intervals_o_salt['at_pos'][i] + four_intervals_o_salt['num_confuse_ap'][i]))
            salt_instruct.append(str(four_intervals_o_salt['len'][i] + four_intervals_o_salt['num_confuse_len'][i]))
        confuse_nums_merge = [] # merging confusion number values together and appending it to the end of salting instructions
        confuse_nums_merge.append(''.join([str(x) for x in four_intervals_o_salt['num_confuse_ap']]))
        confuse_nums_merge.append(''.join([str(x) for x in four_intervals_o_salt['num_confuse_len']]))
        salt_instruct.append(''.join(confuse_nums_merge))
        user_sub_path = f"AWS/users/{_username}"
        os.makedirs(user_sub_path, exist_ok=True)
        ss_sub_path = f"AWS/users/{_username}/SecretSalt"
        os.makedirs(ss_sub_path, exist_ok=True)
        salt_file_path = os.path.join('AWS', 'users', _username, 'SecretSalt', f'secretSalt_{_username}.txt')
        with open(salt_file_path, 'w') as f:
            f.write('!'.join(salt_instruct)) #write salting instructions to user saltFile, with an '!' between each instruction

    else: #decrypting salting instructions, and then salting unecrypted, unsalted password
        four_intervals_o_salt = {'at_pos': [1, 3, 8, 10], 'salt': [4, 5, 6, 7], 'len': [0, 2, 9, 11], 'num_confuse_ap': [], 'num_confuse_len': []}
        split_salt = post_salt.split('!')
        for i in range(0, 12, 3):
            four_intervals_o_salt['num_confuse_ap'].append(str(split_salt[-1])[i:(i + 3)])
            four_intervals_o_salt['num_confuse_len'].append(str(split_salt[-1])[(i + 12):(i + 15)])
        four_intervals_o_salt['at_pos'] = [split_salt[i] for i in four_intervals_o_salt['at_pos']]
        four_intervals_o_salt['at_pos'] = [int(four_intervals_o_salt['at_pos'][x]) - int(four_intervals_o_salt['num_confuse_ap'][x]) for x in range(4)]
        four_intervals_o_salt['salt'] = [split_salt[i] for i in four_intervals_o_salt['salt']]
        four_intervals_o_salt['len'] = [split_salt[i] for i in four_intervals_o_salt['len']]
        four_intervals_o_salt['len'] = [int(four_intervals_o_salt['len'][x]) - int(four_intervals_o_salt['num_confuse_len'][x]) for x in range(4)]

    for x in range(4): #inserting salts into unecnrypted password
        positions = four_intervals_o_salt['at_pos']
        salts = four_intervals_o_salt['salt']
        if x == 0:
            salted_pass.append(_password[0:positions[x]])
            salted_pass.append(salts[x])
        elif x < 3:
            salted_pass.append(_password[positions[x-1]:positions[x]])
            salted_pass.append(salts[x])
        else:
            salted_pass.append(_password[positions[x-1]:len(_password)])
            salted_pass.append(salts[x])
    return ''.join(salted_pass) # return the salted password

def encrypt_password(password):
    """Encrypts password using Utf-8 encoding and SHA-256"""
    # Utf-8 encode the password
    password_bytes = password.encode('utf-8')
    
    # Hash the password using SHA-256
    hashed_password = hashlib.sha256(password_bytes).hexdigest()
    
    # Return the hashed password (encoded as a hex string for storage)
    return hashed_password

def create_creds(unused_param):
    """Creates a new user with defined username and password comhbination. 'unsued_param' exists since each function
    called by the terminal object in terminal.py must have an equivalent number of function parameters so that a parameter
    error is not thrown"""
    username = input('Please enter a new username: ')
    password = getpass.getpass('Please enter a password (must be a minimum of 16 characters long): ')
    while len(password) < 16:
        print('YOUR PASSWORD MUST BE AT LEAST 16 CHARACTERS LONG!')
        password = getpass.getpass('Please enter a password: ')
    password = encrypt_password(salt_pass(username, password))
    persist_creds(username, password) # after getting password and username, persist credentials in the DB

def delete_user(unused_param):
    """Verifies a combination of username, password, and secret salt string credentials and secret salt instruction string
    exist in the database, then deletes the user"""
    username = input('Please enter your username: ')
    password = getpass.getpass('Please enter your password: ')
    salt = input('Please enter your unique salt value: ')
    password = encrypt_password(salt_pass(username, password, salt)) #encrypt password according to salt value provided
    result = create_db_connection(text(f"SELECT 1 FROM user_credentials WHERE username = '{username}' AND password = '{password}'"), return_result=True)
    if 1 in result: # if 1 is returned, we know that the combination exists, login successful, return True and username for acct terminal entry
        print(f'Are you sure you want to delete user {username}? Please note that this action cannot be undone, and all resources related to this account will be lost!')
        confirm_delete = input('Type CONFIRM to proceed with user and resource deletion: ')
        if confirm_delete == 'CONFIRM': # if delete is confirmed, remove entry from user credentials (this should cascade and delete any rows in related tables for this user) and remove user subdirectory
            create_db_connection(row_action('user_credentials', ['user_id'], [name_to_id('user_credentials', 'user_id', 'username', username)], 'DELETE'))
            shutil.rmtree(f"AWS/users/{username}")
            print(f'User {username} and any related resources have successfully been deleted')
    else: # otherwise, return False and login not successful
        print('ERROR: The user/password combination was not found!')

def reset_password(_username):
    """Asks for a new password, then updates credentials and secretSalt file accordingly"""
    new_password = getpass.getpass('Please enter a new password: ')
    while len(new_password) < 16:
        print('YOUR PASSWORD MUST BE AT LEAST 16 CHARACTERS LONG!')
        new_password = getpass.getpass('Please enter a password: ')
    new_password = encrypt_password(salt_pass(_username, new_password)) #resalt password, create new secretSalt file
    update_creds(_username, new_password) #update credentials in the DB

def verify_login():
    """Verifies that a combination of username, password, and secret salt string credentials and secret salt instruction string
    exist in the database"""
    username = input('Please enter your username: ')
    password = getpass.getpass('Please enter your password: ')
    salt = input('Please enter your unique salt value: ')
    password = encrypt_password(salt_pass(username, password, salt)) #encrypt password according to salt value provided
    result = create_db_connection(text(f"SELECT 1 FROM user_credentials WHERE username = '{username}' AND password = '{password}'"), return_result=True)
    if 1 in result: # if 1 is returned, we know that the combination exists, login successful, return True and username for acct terminal entry
        print('LOGIN WAS SUCCESSFULL! HAVE A GOOD DAY MATE!')
        return [True, username]
    else: # otherwise, return False and login not successful
        print('EITHER YOUR USERNAME OR PASSWORD WAS NOT VALID MATE!')
        return [False, username]

def aws_login(transfer_func):
    """Entry point into account level terminal if successful login, verifies login and then passes necessary information 
    to the terminal transfer function (transfer_func parameter) defined in terminal.py, in this case the terminal will be 'acct'"""
    valid_login = verify_login()
    if valid_login[0]:
        print(f'Welcome {valid_login[1]}! Please select a service to get started :)')
        transfer_func(valid_login[1])
import random
import hashlib
import logging
import getpass
import os
from config import DATABASE, HOST, USER, PASSWORD
from sqlalchemy import create_engine, text
from sql_helper import create_row, update_row, check_if_user_exists

def persist_creds(_username, _password):
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    with engine.connect() as conn:
        sql = check_if_user_exists('user_credentials', _username)
        try:
            result = conn.execute(sql)
        except:
            conn.rollback()
            print(f'There was a sql error with the following query: {sql}')
        if len([row for row in result]) > 0:
            print(f'ERROR! Username {_username} is already taken, please choose a different username!')
            return False
        else:
            sql = create_row('user_credentials', ['username', 'password'], [f"'{_username}'", f"'{_password}'"])
            try:
                conn.execute(sql)
                conn.commit()
            except:
                conn.rollback()
                print(f'There was a sql error with the following query: {sql}')

def update_creds(_username, _password):
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    with engine.connect() as conn:
        sql = update_row('user_credentials', 'password', _password, 'username', _username)
        try:
            conn.execute(sql)
            conn.commit()
        except Exception as e:
            logging.error('did not work: ', e)
            conn.rollback()

def salt_pass(_username, _password, post_salt='', clear_slt_file=False):
    salted_pass = []
    if clear_slt_file:
        user_sub_path = f"AWS/users/{_username}"
        ss_sub_path = f"AWS/users/{_username}/SecretSalt"
        salt_file_path = os.path.join('AWS', 'users', _username, 'SecretSalt', f'secretSalt_{_username}.txt')
        with open(salt_file_path, 'w') as f:
            pass
    if post_salt == '':
        four_intervals_o_salt = {'at_pos': [], 'salt': [], 'len': [], 'num_confuse_ap': [random.randint(123, 321) for _ in range(4)], 'num_confuse_len': [random.randint(123, 321) for _ in range(4)]}
        for i in range(4):
            four_intervals_o_salt['at_pos'].append(random.randint(1, 4) + (4 * i))
            salt_start = []
            m = random.randint(1, 4)
            four_intervals_o_salt['len'].append(m)
            for z in range(m):
                salt_start.append(chr(random.randint(34, 126)))
            four_intervals_o_salt['salt'].append(''.join(salt_start))
        
        salt_instruct = []
        for i in range(2):
            salt_instruct.append(str(four_intervals_o_salt['len'][i] + four_intervals_o_salt['num_confuse_len'][i]))
            salt_instruct.append(str(four_intervals_o_salt['at_pos'][i] + four_intervals_o_salt['num_confuse_ap'][i]))
        for i in range(4):
            salt_instruct.append(str(four_intervals_o_salt['salt'][i]))
        for i in range(2, 4):
            salt_instruct.append(str(four_intervals_o_salt['at_pos'][i] + four_intervals_o_salt['num_confuse_ap'][i]))
            salt_instruct.append(str(four_intervals_o_salt['len'][i] + four_intervals_o_salt['num_confuse_len'][i]))
        confuse_nums_merge = []
        confuse_nums_merge.append(''.join([str(x) for x in four_intervals_o_salt['num_confuse_ap']]))
        confuse_nums_merge.append(''.join([str(x) for x in four_intervals_o_salt['num_confuse_len']]))
        salt_instruct.append(''.join(confuse_nums_merge))
        user_sub_path = f"AWS/users/{_username}"
        os.makedirs(user_sub_path, exist_ok=True)
        ss_sub_path = f"AWS/users/{_username}/SecretSalt"
        os.makedirs(ss_sub_path, exist_ok=True)
        salt_file_path = os.path.join('AWS', 'users', _username, 'SecretSalt', f'secretSalt_{_username}.txt')
        with open(salt_file_path, 'w') as f:
            f.write('!'.join(salt_instruct))

    else:
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

    for x in range(4):
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
    return ''.join(salted_pass)

def encrypt_password(password):
    # Utf-8 encode the password
    password_bytes = password.encode('utf-8')
    
    # Hash the password using SHA-256
    hashed_password = hashlib.sha256(password_bytes).hexdigest()
    
    # Return the hashed password (encoded as a hex string for storage)
    return hashed_password

def create_creds(unused_param):
    username = input('Please enter a new username: ')
    password = getpass.getpass('Please enter a password (must be a minimum of 16 characters long): ')
    while len(password) < 16:
        print('YOUR PASSWORD MUST BE AT LEAST 16 CHARACTERS LONG!')
        password = getpass.getpass('Please enter a password: ')
    password = encrypt_password(salt_pass(username, password))
    persist_creds(username, password)

def reset_password(_username):
    new_password = getpass.getpass('Please enter a new password: ')
    while len(new_password) < 16:
        print('YOUR PASSWORD MUST BE AT LEAST 16 CHARACTERS LONG!')
        new_password = getpass.getpass('Please enter a password: ')
    new_password = encrypt_password(salt_pass(_username, new_password))
    update_creds(_username, new_password)

def verify_login():
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    username = input('Please enter your username: ')
    password = getpass.getpass('Please enter your password: ')
    salt = input('Please enter your unique salt value: ')
    password = encrypt_password(salt_pass(username, password, salt))
    sql = text(f"SELECT 1 as \"password_correct\" FROM user_credentials WHERE username = '{username}' AND password = '{password}'")
    with engine.connect() as conn:
        try:
            result = conn.execute(sql)
            for row in result:
                print('LOGIN WAS SUCCESSFULL! HAVE A GOOD DAY MATE!')
                return [True, username]
            else:
                print('EITHER YOUR USERNAME OR PASSWORD WAS NOT VALID MATE!')
                return [False, username]
        except Exception as e:
            logging.error('did not work: ', e)
            # print(f"THERE WAS AN ERROR WITH THE FOLLOWING SQL SCRIPT: {sql}")
            conn.rollback()

def aws_login(transfer_func):
        valid_login = verify_login()
        if valid_login[0]:
            print(f'Welcome {valid_login[1]}! Please select a service to get started :)')
            transfer_func(valid_login[1])
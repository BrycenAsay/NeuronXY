import sys
import shutil
from sqlalchemy import text
from account_creation import verify_login, create_creds, reset_password
from s3 import s3_bucket, set_vv_abap, upload_properties_to_db, create_s3_directory, remove_bucket_db_dir, update_bucket, persist_object
from sql_helper import create_db_connection, name_to_id

def admin_level_main():
    f_command = sys.argv[1]
    s_command = sys.argv[2]
    if not f_command:
        exit()
    if f_command == 'aws':
        if s_command == 'login':
            valid_login = verify_login()
            if valid_login[0]:
                print(f'Welcome {valid_login[1]}! Please select a service to get started :)')
                acct_level_main(valid_login[1])
        elif s_command == 'create':
            if sys.argv[3] == 'user':
                create_creds()

def acct_level_main(username):
    prompt_command = ''
    while prompt_command != ['logout']:
        prompt_command = input(f'AWS\\uers\\{username}> ').split(' ')
        if prompt_command[0] == '-res':
            if prompt_command[1] == 'password':
                reset_password(username)
        if prompt_command[0] == '-srv':
            if prompt_command[1] == 's3':
                s3_def_terminal(username)

def s3_def_terminal(_username):
    prompt_command = ''
    while prompt_command != ['exit']:
        prompt_command = input(f'AWS\\uers\\{_username}\\s3> ').split(' ')
        if prompt_command[0] == '-sel':
            if prompt_command[1] == 'bucket':
                try:
                    bucket_name = prompt_command[2]
                    user_id = name_to_id('user_credentials', 'user_id', 'username', _username)
                    bucket_id = name_to_id('s3', 'bucket_id', 'name', bucket_name)
                    existing_bucket = s3_bucket()
                    for attribute in existing_bucket.properties:
                        existing_bucket.define_bucket_properties(attribute, [x[0] for x in create_db_connection(text(f"SELECT {attribute} FROM s3 WHERE user_id = {user_id} AND bucket_id = {bucket_id}"), return_result=True)][0])
                    s3_bucket_terminal(_username, existing_bucket)
                except:
                    print(f'THERE IS NO BUCKET UNDER NAME {bucket_name} FOR USER {_username}!')
        if prompt_command[0] == '-mk':
            if prompt_command[1] == 'bucket':
                new_bucket = s3_bucket()
                new_bucket = set_vv_abap(new_bucket, 'name')
                ow_def_set = input('Override default settings? (Y/N): ')
                while ow_def_set not in ['Y', 'N']:
                    print('ERROR! ENTER Y or N!')
                    ow_def_set = input('Override default settings? (Y/N): ')
                if ow_def_set == 'Y':
                    print(f'Avaliable changeable properties: {','.join(new_bucket.properties)}\n')
                    def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
                    while def_pv_to_change != 'DONE':
                        try:
                            new_bucket = set_vv_abap(new_bucket, def_pv_to_change)
                        except:
                            pass
                        print(f'Avaliable changeable properties: {','.join(new_bucket.properties)}\n')
                        def_pv_to_change = input('Please enter a default property/value to change. If a valid setting not specified, you will be returned to this prompt. Enter DONE to confirm settings> ')
                upload_properties_to_db(new_bucket, _username)
                create_s3_directory(_username, new_bucket)
        elif prompt_command[0] == '-del':
            if prompt_command[1] == 'bucket':
                bucket_name = prompt_command[2]
                try:
                    remove_bucket_db_dir([x[0] for x in create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)][0]
                                        ,[x[0] for x in create_db_connection(text(f"SELECT bucket_id FROM s3 WHERE name = '{bucket_name}'"), return_result=True)][0])
                    shutil.rmtree(f"AWS/users/{_username}/s3/{bucket_name}")
                except:
                    print(f'A VALID BUCKET NAME FOR THIS USER MUST BE SPECIFIED! BUCKET DELETE UNSUCCESSFUL!')
        elif prompt_command[0] == '-updt':
            if prompt_command[1] == 'bucket':
                bucket_name = prompt_command[2]
                update_bucket([x[0] for x in create_db_connection(text(f"SELECT user_id FROM user_credentials WHERE username = '{_username}'"), return_result=True)][0]
                                ,[x[0] for x in create_db_connection(text(f"SELECT bucket_id FROM s3 WHERE name = '{bucket_name}'"), return_result=True)][0])
        elif prompt_command[0] == '-ls':
            if prompt_command[1] == 'bucket':
                user_id = [x[0] for x in create_db_connection(text(f"SELECT user_id from user_credentials WHERE username = '{_username}'"), return_result=True)][0]
                for bucket_name in [x[0] for x in create_db_connection(text(f"SELECT name FROM s3 WHERE user_id = {user_id}"), return_result=True)]:
                    print(f'{bucket_name}')
            elif prompt_command[1] == 'bucket_settings':
                dummy_bucket = s3_bucket()
                for property in dummy_bucket.properties:
                    print(f'{property}')

def s3_bucket_terminal(_username, bucket):
    prompt_command = ''
    while prompt_command != ['exit']:
        prompt_command = input(f'AWS\\users\\{_username}\\s3\\{bucket.name}> ').split(' ')
        if prompt_command[0] == '-upld':
            object_name = prompt_command[1]
            source = f"AWS/externalFiles/{object_name}"
            destination = f"AWS/users/{_username}/s3/{bucket.name}/"
            shutil.move(source, destination)
            persist_object(bucket, _username, object_name, destination)

    s3_def_terminal(_username)
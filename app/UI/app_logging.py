from helper_scripts.sql_helper import create_row, create_db_connection, name_to_id, postgres_format
import json
from datetime import datetime, date
import pytz

def cur_timestamp_mt():
    # Set Mountain Time
    mountain = pytz.timezone('America/Denver')

    # Get current time in Mountain Time
    now_mt = datetime.now(mountain)

    # Remove timezone info to make it naive
    naive_now_mt = now_mt.replace(tzinfo=None)

    # Format it for PostgreSQL (e.g., '2025-04-22 14:32:00')
    return naive_now_mt.strftime("%Y-%m-%d %H:%M:%S")

def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    else:
        return obj

def create_object_json(raw_obj_instance):
    if raw_obj_instance == None:
        return None
    else:
        dict_obj = {}
        keys = raw_obj_instance.properties
        for key in keys:
            dict_obj[key] = json_serial(getattr(raw_obj_instance, key))
    return json.dumps(dict_obj)

def create_log_entry(_username, action, description, srv_name=None, host_name=None, raw_host=None, object_name=None, raw_object=None):
    cols = ['created_timestamp', 'description', 'user_id', 'service', 'action', 'host', 'host_details', 'object', 'object_details']
    user_id = name_to_id('user_credentials', 'user_id', 'username', _username)
    log_timestamp = cur_timestamp_mt()
    host_json = create_object_json(raw_host)
    object_json = create_object_json(raw_object)
    log_data = postgres_format([log_timestamp, description, user_id, srv_name, action, host_name, host_json, object_name, object_json])
    create_db_connection(create_row('logging', cols, log_data))
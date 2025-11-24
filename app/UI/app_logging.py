from helper_scripts.sql_helper import create_row, row_action, create_db_connection, name_to_id, postgres_format, raw_sql
from helper_scripts.utils import create_object_json
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

def create_log_entry(_username, action, description, srv_name=None, host_name=None, raw_host=None, object_name=None, raw_object=None, synapse_run=False):
    from synapse.synapse_qf import synapse_log_reader # importing within function to avoid a circular import :/

    cols = ['created_timestamp', 'description', 'user_id', 'service', 'action', 'host', 'host_details', 'object', 'object_details', 'synapse_processed']
    user_id = name_to_id('user_credentials', 'user_id', 'username', _username)
    log_timestamp = cur_timestamp_mt()
    host_json = create_object_json(raw_host)
    object_json = create_object_json(raw_object)
    log_data = postgres_format([log_timestamp, description, user_id, srv_name, action, host_name, host_json, object_name, object_json, False])
    create_db_connection(create_row('logging', cols, log_data))
    if raw_host is not None:
        if (srv_name == 'cortex' and (create_db_connection(raw_sql(f'SELECT COUNT(*) FROM synapse_qf WHERE in_srv_id = {name_to_id('cortex', 'node_id', 'nrn', getattr(raw_host, 'nrn'))}'), return_result=True)[0] > 0 and not synapse_run)):
            synapse_log_reader(user_id, srv_name, action, host_name, object_name)
    create_db_connection(row_action('', ['synapse_processed'], ['False'], f'UPDATE logging SET synapse_processed = True', frm_keywrd=''))
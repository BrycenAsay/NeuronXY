import duckdb
import pandas as pd
from pathlib import Path
from typing import Literal
from cortex.cortex import ls_node
from helper_scripts.sql_helper import create_db_connection, row_action, create_row, name_to_id, postgres_format
import os

y_or_n_input = lambda x: True if x == 'Y' else False # returns true if value is 'Y' (yes) otherwise defaults to False

class synapse_fq:
    """synapse function/query object, attributes for a synapse function/query currently include:
    
    name: the name for the function/query
    in_srv_type: the service type that is being monitored for a trigger
    in_srv_host: the name of the service host to monitor for the trigger
    in_trig_type: the type of trigger being monitored for (POST, GET, DELETE, etc)
    in_trig_object: the resource being effected by the trigger type, i.e. a file for a cortex node POST
    out_srv_type: the output service type
    src_file: the .sql or .py file responsible for preforming the action
    timeout: the total amount of time to allow a function to run before exiting
    enabled: wether or not the query/function is currently enabled for the trigger type specified
    """
    def __init__(self,
                 properties = ['name', 'in_srv_type', 'in_srv_host', 'in_trig_type', 'in_trig_object', 'out_srv_type', 'out_srv_host', 'out_trig_type', 'out_trig_object', 'src_file', 'timeout', 'enabled'],
                 name = '',
                 in_srv_type = '',
                 in_srv_host = '',
                 in_trig_type = '',
                 in_trig_object = '',
                 out_srv_type = '',
                 out_srv_host = '',
                 out_trig_type = '',
                 out_trig_object = '',
                 src_file = '',
                 timeout = 200,
                 enabled = 'Y'):
        self.properties = properties
        self.name = name
        self.in_srv_type = in_srv_type
        self.in_srv_host = in_srv_host
        self.in_trig_type = in_trig_type
        self.in_trig_object = in_trig_object
        self.out_srv_type = out_srv_type
        self.out_srv_host = out_srv_host
        self.out_trig_type = out_trig_type
        self.out_trig_object = out_trig_object
        self.src_file = src_file
        self.timeout = timeout
        self.enabled = enabled

    def define_fq_properties(self, attr_name, value):
        if hasattr(self, attr_name):  # Ensure the attribute exists
            setattr(self, attr_name, value)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")
        
    def get_fq_properties(self, attr_name):
        """returns the current node property value for a given attribute"""
        if hasattr(self, attr_name):  # Ensure the attribute exists
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr_name}'")

def prompt_validation(prompt:str, req_vals=[], prnt_req_vals=False, req_dtype=False):
    if prnt_req_vals:
        print(', '.join(req_vals))
    out_val = input(prompt)
    if req_vals:
        while out_val not in req_vals:
            if prnt_req_vals:
                print(', '.join(req_vals))
            out_val = input(prompt)
    elif req_dtype:
        while not isinstance(out_val, req_dtype):
            out_val = input(prompt)
    return out_val

def sanitize_filename(filename):
    # Replace problematic characters with underscores
    invalid_chars = r'<>:"/\|?*$&;()[]{}^!#%@'
    for char in invalid_chars:
        filename = filename.replace(char, '')
    # Trim spaces and dots at beginning/end
    return filename.strip(' .')

def qf_file_creator(qf_type=Literal['function', 'query']):
    current_dir = Path(__file__).parent
    if qf_type == 'function':
        selector_path = 'pandas_funcs'
    else:
        selector_path = 'duckdb_scripts'
    qf_filename = sanitize_filename(input(f'Please enter a name for this {qf_type} between 1 and 64 characters. If you want to exit type EXIT: '))
    while not (1 <= len(qf_filename) <= 64):
        qf_filename = sanitize_filename(input(f'Please enter a name for this {qf_type} between 1 and 64 characters. If you want to exit type EXIT: '))
    if qf_filename == 'EXIT':
        return None
    if qf_type == 'function':
        qf_filename = f'{qf_filename}.py'
    elif qf_type == 'query':
        qf_filename = f'{qf_filename}.sql'
    with open(f'{current_dir}/{selector_path}/{qf_filename}', 'w') as f:
        if qf_type == 'function':
            f.write('return_df = df.copy() # DO NOT DELETE THIS LINE, AND PREFORM TRANSFORMS TO return_df VARIABLE WITHOUT RENAMING return_df!')
        elif qf_type == 'query':
            f.write('SELECT * FROM return_arrow_table a --DO NOT RENAME THIS TABLE! However, you may rename the table alias as you wish :)')
    return qf_filename

def upload_properties_to_db(fq, _username):
    """Uploads node properties to the DB"""
    cols = ['user_id'] + fq.properties
    cols[cols.index('in_srv_host')] = 'in_srv_id'
    cols[cols.index('out_srv_host')] = 'out_srv_id'
    prepro_vals = [name_to_id('user_credentials', 'user_id', 'username', _username)]
    vals = []
    for property in fq.properties:
        prepro_vals.append(fq.get_fq_properties(property))
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
    create_db_connection(create_row('synapse_qf', cols, vals))

def mk_synapse_qf(_username):
    qf = prompt_validation('Please enter Synapse type (function or query): ', req_vals=['function', 'query'])
    filename = qf_file_creator(qf)
    if filename == None:
        return
    host_types_id = {'cortex': 'node_id'}
    host_types_name = {'cortex': 'name'}
    object_types = {'cortex': ['file']}
    in_srv_type = prompt_validation('Please enter service type to be monitored for a trigger: ', req_vals=['cortex'], prnt_req_vals=True)
    in_srv_host = name_to_id(in_srv_type, host_types_id[in_srv_type], host_types_name[in_srv_type], prompt_validation(f'Please enter the name of the resource host to monitor: ', req_vals=ls_node(_username, None, None, return_list=True), prnt_req_vals=True))
    in_trig_type = prompt_validation('Please enter the type of trigger to be monitored for: ', req_vals=['PUT', 'POST'], prnt_req_vals=True)
    in_trig_object = prompt_validation('Please enter the resource being monitored for the trigger action: ', req_vals=object_types[in_srv_type], prnt_req_vals=True)
    out_srv_type = prompt_validation('Please enter service type to output qf results: ', req_vals=['cortex'], prnt_req_vals=True)
    out_srv_host = name_to_id(out_srv_type, host_types_id[out_srv_type], host_types_name[out_srv_type], prompt_validation(f'Please enter the name of the host to preform synapse qf action on: ', req_vals=ls_node(_username, None, None, 
                                                                                                                                                                                                       exclude_nodes=[name_to_id('cortex', 'node_id', 'name', in_srv_host, reversed=True)], return_list=True), prnt_req_vals=True))
    out_trig_type = prompt_validation('Please enter the type of action that will preformed after the synapse qf has ran: ', req_vals=['PUT', 'POST'], prnt_req_vals=True)
    out_trig_object = prompt_validation('Please enter the resource from the action triggered after the syanspe qf has ran: ', req_vals=object_types[in_srv_type], prnt_req_vals=True)
    synapse_fq_object = synapse_fq(name=filename.split('.')[0],
                                   in_srv_type=in_srv_type,
                                   in_srv_host=in_srv_host,
                                   in_trig_type=in_trig_type,
                                   in_trig_object=in_trig_object,
                                   out_srv_type=out_srv_type,
                                   out_srv_host=out_srv_host,
                                   out_trig_type=out_trig_type,
                                   out_trig_object=out_trig_object,
                                   src_file=filename)
    upload_properties_to_db(synapse_fq_object, _username)

def synapse_log_reader(user_id, srv_type, srv_id, in_trig_type, in_object_type):
    retrival_data = postgres_format([user_id, srv_type, srv_id, in_trig_type, in_object_type, True])
    synapse_funcs = create_db_connection(row_action('synapse', ['user_id', 'in_srv_type', 'in_srv_id', 'in_trig_type', 'in_trig_object', 'enabled'], retrival_data, 'SELECT out_srv_type, out_srv_host, out_trig_type, out_trig_object, src_file'), multi_return=[True, 5])
    
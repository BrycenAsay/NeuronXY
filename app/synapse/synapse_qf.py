from pathlib import Path
from typing import Literal
import json
from cortex.cortex import ls_node, sel_node, cortex_node
from cortex.cortex_node import cortex_file, get_file, upload_file
from helper_scripts.sql_helper import create_db_connection, row_action, create_row, name_to_id, postgres_format
from helper_scripts.utils import init_object, process_df_using_file, process_arw_using_file, create_object_json
import logging

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

def inst_obj(srv, obj_type, obj_dict):
    try:
        if srv == 'cortex':
            if obj_type == 'node':
                return init_object(cortex_node, obj_dict)
            elif obj_type == 'file':
                return init_object(cortex_file, obj_dict)
    except Exception as e:
        logging.error(e, exc_info=True)

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
            f.write('def process_df(_df): # DO NOT RENAME THIS FUNCTION OR IT\'S PARAMETERS!\n')
            f.write('\treturn_df = _df.copy() # DO NOT DELETE THIS LINE, AND PREFORM TRANSFORMS TO return_df VARIABLE WITHOUT RENAMING return_df!\n')
            f.write('\n# Use this space to preform transforms on the df\n')
            f.write('\treturn return_df # DO NOT DELETE THIS LINE, AND PREFORM TRANSFORMS TO return_df VARIABLE WITHOUT RENAMING return_df!')
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
    try:
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
    except Exception as e:
        logging.error(e, exc_info=True)

def create_fq_obj(ids, data, replace_keys):
    try:
        raw_obj_dict = {}
        dummy_qf = synapse_fq()
        for prop in dummy_qf.properties:
            if prop in replace_keys:
                raw_obj_dict[prop] = create_db_connection(row_action('synapse_qf', ids, postgres_format(data), f'SELECT {replace_keys[prop]}'), return_result=True)
            else:
                raw_obj_dict[prop] = create_db_connection(row_action('synapse_qf', ids, postgres_format(data), f'SELECT {prop}'), return_result=True)
        return init_object(synapse_fq, raw_obj_dict)
    except Exception as e:
        logging.error(e, exc_info=True)

def fq_run(user_id, srv, host, object, synapse_qf):
    try:
        return_type = lambda x: 'pandas_df' if x == 'py' else 'arrow_table'
        if srv == 'cortex':
            if synapse_qf.get_fq_properties('out_trig_type')[0] == 'POST':
                if (synapse_qf.get_fq_properties('enabled')[0] is True and synapse_qf.get_fq_properties('in_srv_host')[0] == name_to_id('cortex', 'node_id', 'name', host.get_node_properties('name'))):
                    if object is not None:
                        raw_file = get_file(user_id, name_to_id('cortex', 'node_id', 'nrn', host.get_node_properties('nrn')), name_to_id('cortex_node', 'file_id', 'hdfs_path', object.get_file_properties('hdfs_path')), return_type(synapse_qf.get_fq_properties('src_file')[0].split('.')[1]))
                    if synapse_qf.get_fq_properties('src_file')[0].split('.')[1] == 'py':
                        pro_file = process_df_using_file(raw_file, synapse_qf.get_fq_properties('src_file')[0])
                    elif synapse_qf.get_fq_properties('src_file')[0].split('.')[1] == 'sql':
                        pro_file = process_arw_using_file(raw_file, synapse_qf.get_fq_properties('src_file')[0])
                    upload_file(name_to_id('user_credentials', 'user_id', 'username', user_id, True), 
                                sel_node(name_to_id('user_credentials', 'user_id', 'username', user_id, True), name_to_id('cortex', 'node_id', 'name', synapse_qf.get_fq_properties('out_srv_host')[0], reversed=True), None, override_transfer=True), 
                                getattr(object, 'hdfs_path').split('/')[-1], pa_upload=True, pa_table=pro_file, bp_input=True, synapse_run=True)
    except Exception as e:
        logging.error(e, exc_info=True)

def synapse_log_reader(user_id, srv, act, hst, obj):
    try:
        unpro_hst_obj = []
        log_data = postgres_format([user_id, srv, act, hst, obj, False])
        unpro_hst_obj_raw = create_db_connection(row_action('logging', ['user_id', 'service', 'action', 'host', 'object', 'synapse_processed'], log_data, 'SELECT host, host_details, object, object_details'), multi_return=[True, 4])
        for i in range(len(unpro_hst_obj_raw[0])):
            unpro_hst_obj.append([lyst[i] for lyst in unpro_hst_obj_raw])
        hsts_objs = {'host': [], 'object': []}
        for hst_obj in unpro_hst_obj:
            if hst_obj[1] is not None:
                hsts_objs['host'].append(inst_obj(srv, hst_obj[0], hst_obj[1]))
            if hst_obj[3] is not None:
                hsts_objs['object'].append(inst_obj(srv, hst_obj[2], hst_obj[3]))
        if hsts_objs['host'] != []:
            for i in range(len(hsts_objs['host'])):
                synapse_qf = create_fq_obj(['user_id', 'in_srv_type', 'in_srv_id', 'in_trig_type', 'in_trig_object'], [user_id, srv, 
                                                        name_to_id('cortex', 'node_id', 'nrn', getattr(hsts_objs['host'][i], 'nrn')), act, obj], {'in_srv_host': 'in_srv_id', 'out_srv_host':'out_srv_id'})
                fq_run(user_id, srv, hsts_objs['host'][i], hsts_objs['object'][i], synapse_qf)
    except Exception as e:
        logging.error(e, exc_info=True)
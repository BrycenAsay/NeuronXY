import importlib.util
from datetime import datetime, date
import json
import duckdb
import os

sanitize_num_input = lambda x: int(x) if x.isdigit() else -999999

def prompt_validation(prompt:str, req_vals=[], bool_eval:dict={}, req_num_range=[], req_len_range=[], prnt_req_vals=False, req_dtype=False, bp_input=[False, '']):
    if bp_input[0]:
        return bp_input[1]
    if prnt_req_vals:
        print(', '.join(req_vals))
    out_val = input(prompt)
    if req_vals:
        while out_val not in req_vals:
            if prnt_req_vals:
                print(', '.join(req_vals))
            out_val = input(prompt)
        if bool_eval:
            return bool_eval[out_val]
    elif req_dtype:
        while not isinstance(out_val, req_dtype):
            out_val = input(prompt)
    elif req_num_range:
        while not req_num_range[0] <= sanitize_num_input(out_val) <= req_num_range[1]:
            out_val = input(prompt)
    elif req_len_range:
        while not req_len_range[0] <= len(out_val) <= req_len_range[1]:
            out_val = input(prompt)
    return out_val

def init_object(class_obj, obj_dict):
    obj_inst = class_obj()
    for key, item in obj_dict.items():
        setattr(obj_inst, key, item)
    return obj_inst

def process_df_using_file(_df, _src_file):
    # Load the module dynamically
    abs_srcfile_path = os.path.abspath(f'/app/synapse/pandas_funcs/{_src_file}')
    spec = importlib.util.spec_from_file_location("pandas_qf", abs_srcfile_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Now use the function from the dynamically loaded module
    return module.process_df(_df)

def process_arw_using_file(arwtbl, _src_file):
    con = duckdb.connect(database=':memory:')
    con.register('return_arrow_table', arwtbl)

    sql_file_path = os.path.abspath(f'/app/synapse/duckdb_scripts/{_src_file}')
    with open(sql_file_path, 'r') as f:
        sql_query = f.read()
    result = con.execute(sql_query)

    result_arrow = result.fetch_arrow_table()
    con.close()

    return result_arrow

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
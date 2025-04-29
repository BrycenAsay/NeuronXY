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

def init_object(class_obj, **kwargs):
    for key, item in kwargs.items():
        class_obj.setattr(key, item)
    return class_obj
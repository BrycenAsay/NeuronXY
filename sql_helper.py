from sqlalchemy import text, create_engine
from config import USER, PASSWORD, HOST, PORT, DATABASE
import logging

def create_db_connection(_sql, return_result:bool = False, multi_return:list = [False, 0]):
    """Creates a database connection and runs a specified sql command. Returns results if return_result is set to True, it is False by default"""
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')
    with engine.connect() as conn:
        try:
            if return_result:
                result = conn.execute(_sql).fetchall()
                return [row[0] for row in result]
            elif multi_return[0]:
                result = conn.execute(_sql).fetchall()
                mast_list = []
                for i in range(multi_return[1]):
                    mast_list.append([row[i] for row in result])
                return mast_list
            else:
                conn.execute(_sql)
                conn.commit()
        except:
            conn.rollback()
            print(f'There was a sql error with the following query: {_sql}')

def create_row(table_name, columns:list, data:list):
    """Creates a row in the database given a table, a list of columns, and a list of values for specified columns"""
    columns = ",".join(columns)
    data = ",".join(data)
    query = text(f'INSERT INTO {table_name}({columns}) VALUES ({data});')
    return query
    
def row_action(table_name, ids:list, values:list, action_type, not_eq:list=[], frm_keywrd='FROM', group_state='', order_state='', limit_state=''):
    """Accepts a list of ids and correlated values with those ids to delete entires from a specified table"""
    values = ['Null' if x is None else x for x in values]
    ids = ['Null' if x is None else x for x in ids]
    where_id_eq_val = []
    while len(not_eq) != len(values):
        not_eq.append(False)
    for i in range(len(ids)):
        if values[i] == 'Null':
            no_eq_op = 'is not'
            eq_op = 'is'
        elif isinstance(values[i], list):
            if len(values[i]) == 0:
                continue
            values[i] = f"({', '.join([f"'{val}'" if (isinstance(val, str) and val != 'Null') else val for val in values[i]])})"
            no_eq_op = 'NOT IN'
            eq_op = 'IN'
        else:
            no_eq_op = '!='
            eq_op = '='
        if i == 0:
            if not_eq[i]:
                where_id_eq_val.append(f'WHERE {ids[i]} {no_eq_op} {values[i]}')
            else:
                where_id_eq_val.append(f'WHERE {ids[i]} {eq_op} {values[i]}')
        else:
            if not_eq[i]:
                where_id_eq_val.append(f'AND {ids[i]} {no_eq_op} {values[i]}')
            else:
                where_id_eq_val.append(f'AND {ids[i]} {eq_op} {values[i]}')
    return text(f'{action_type} {frm_keywrd} {table_name} {where_id_eq_val[0]} {' '.join(where_id_eq_val[1:len(where_id_eq_val)])} {group_state} {order_state} {limit_state}')

def update_row(table_name, column, data, where_1, eq_value_1):
    """Updates row given one column equals one value. Must specify column and the associated value as part of the function parameters"""
    if (isinstance(data, int) or data == 'Null'):
        query = text(f"UPDATE {table_name} SET {column} = {data} WHERE {where_1} = '{eq_value_1}'")
    elif isinstance(data, str):
        query = text(f"UPDATE {table_name} SET {column} = '{data}' WHERE {where_1} = '{eq_value_1}'")
    return query

def update_row_dos_id(table_name, column, data, where_id1, eq_value_1, where_id2, eq_value_2):
    """Updates row given two column equals one value. Must specify column and the associated value as part of the function parameters"""
    query = text(f"UPDATE {table_name} SET {column} = {data} WHERE {where_id1} = {eq_value_1} AND {where_id2} = {eq_value_2}")
    return query

def check_if_user_exists(table_name, username):
    """Checks if user exists in a given table"""
    query = text(f"SELECT 1 FROM {table_name} WHERE username = '{username}'")
    return query

def check_if_value_exists(table_name, column_name, value):
    """Checks if value exists in a table where specified column name equals specified value"""
    query = text(f"SELECT 1 FROM {table_name} WHERE {column_name} = '{value}'")
    return query

def name_to_id(table_name, id_column, name_column, value, reversed:bool=False, one_result:bool=True):
    """Converts a name to a numerical ID. Unique INT ids exist for easier DB row management"""
    if value == None:
        value = 'Null'
    try:
        if (reversed and one_result):
            return create_db_connection(text(f"SELECT {name_column} FROM {table_name} WHERE {id_column} = {value}"), return_result=True)[0]
        elif (not reversed and one_result):
            return create_db_connection(text(f"SELECT {id_column} FROM {table_name} WHERE {name_column} = '{value}'"), return_result=True)[0]
        elif (reversed and not one_result):
            return create_db_connection(text(f"SELECT {name_column} FROM {table_name} WHERE {id_column} = {value}"), return_result=True)
        else:
            return create_db_connection(text(f"SELECT {id_column} FROM {table_name} WHERE {name_column} = '{value}'"), return_result=True)
    except:
      return 999999999
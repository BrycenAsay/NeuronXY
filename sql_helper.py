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
    
def row_action(table_name, ids:list, values:list, action_type, frm_keywrd='FROM', group_state='', order_state='', limit_state=''):
    """Accepts a list of ids and correlated values with those ids to delete entires from a specified table"""
    where_id_eq_val = []
    for i in range(len(ids)):
        if i == 0:
            where_id_eq_val.append(f'WHERE {ids[i]} = {values[i]}')
        else:
            where_id_eq_val.append(f'AND {ids[i]} = {values[i]}')
    return text(f'{action_type} {frm_keywrd} {table_name} {where_id_eq_val[0]} {' '.join(where_id_eq_val[1:len(where_id_eq_val)])} {group_state} {order_state} {limit_state}')

def update_row(table_name, column, data, where_1, eq_value_1):
    """Updates row given one column equals one value. Must specify column and the associated value as part of the function parameters"""
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

def name_to_id(table_name, id_column, name_column, name):
    """Converts a name to a numerical ID. Unique INT ids exist for easier DB row management"""
    try:
      return create_db_connection(text(f"SELECT {id_column} FROM {table_name} WHERE {name_column} = '{name}'"), return_result=True)[0]
    except:
      return 999999999
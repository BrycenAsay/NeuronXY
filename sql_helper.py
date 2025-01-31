from sqlalchemy import text, create_engine
from config import USER, PASSWORD, HOST, DATABASE

def create_db_connection(_sql, return_result:bool = False):
    """Creates a database connection and runs a specified sql command. Returns results if return_result is set to True, it is False by default"""
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    with engine.connect() as conn:
        try:
            if return_result:
                result = conn.execute(_sql).fetchall()
                return [row[0] for row in result]
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

def delete_row_two_ids(table_name, id_uno, value_uno, id_dos, value_dos):
    """Given two ids for a row, deletes all columns attached to the two ids. You must specifiy the id and the associated value
    as part of the function parameters"""
    query = text(f'DELETE FROM {table_name} WHERE {id_uno} = {value_uno} AND {id_dos} = {value_dos};')
    return query

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
    return create_db_connection(text(f"SELECT {id_column} FROM {table_name} WHERE {name_column} = '{name}'"), return_result=True)[0]
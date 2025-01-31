from sqlalchemy import text, create_engine
from config import USER, PASSWORD, HOST, DATABASE

def create_db_connection(_sql, return_result:bool = False):
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

def create_row(table_name, columns, data):
    columns = ",".join(columns)
    data = ",".join(data)
    query = text(f'INSERT INTO {table_name}({columns}) VALUES ({data});')
    return query

def delete_row_two_ids(table_name, id_uno, value_uno, id_dos, value_dos):
    query = text(f'DELETE FROM {table_name} WHERE {id_uno} = {value_uno} AND {id_dos} = {value_dos};')
    return query

def update_row(table_name, column, data, where_1, eq_value_1):
    query = text(f"UPDATE {table_name} SET {column} = '{data}' WHERE {where_1} = '{eq_value_1}'")
    return query

def update_row_dos_id(table_name, column, data, where_id1, eq_value_1, where_id2, eq_value_2):
    query = text(f"UPDATE {table_name} SET {column} = {data} WHERE {where_id1} = {eq_value_1} AND {where_id2} = {eq_value_2}")
    return query

def check_if_user_exists(table_name, username):
    query = text(f"SELECT 1 FROM {table_name} WHERE username = '{username}'")
    return query

def check_if_value_exists(table_name, column_name, value):
    query = text(f"SELECT 1 FROM {table_name} WHERE {column_name} = '{value}'")
    return query

def name_to_id(table_name, id_column, name_column, name):
    return create_db_connection(text(f"SELECT {id_column} FROM {table_name} WHERE {name_column} = '{name}'"), return_result=True)[0]
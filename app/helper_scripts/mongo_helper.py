from pymongo import MongoClient, HASHED, ASCENDING, DESCENDING
from config import MGHOST, MGLOCALHOST, MGUSER, MGPASSWORD
import logging
from contextlib import contextmanager

@contextmanager
def mongo_session():
    """Creates a context manager for Mongo to prevent unecessary open session when Mongo operations are completed"""
    client = MongoClient(f'mongodb://{MGUSER}:{MGPASSWORD}@{MGLOCALHOST}:27017/')
    try:
        yield client
    finally:
        client.close()

def create_mongo_db_ins(db_name:str, collection_name:str):
    """Creates a Mongo DB and an empty collection (if db already exists can also just create a new collection as well)"""
    try:
        with mongo_session() as conn:
            db = conn[f'{db_name}']
            collection = db[f'{collection_name}']
    except Exception as e:
        logging.error(e)

def create_collection_indexs(db_name:str, collection_name:str, indexs:list, index_types:list, _unique:bool):
    """Creates an indexing strategy for a given collection.
    
    indexs_list: A list of tuples, the first parameter is the index name, and the second is partioning strategy (HASHED, ASCENDING, or DESCENDING)"""
    index_spec = []
    index_type = {"hashed": HASHED, "asc": ASCENDING,"desc": DESCENDING}
    for x in index_types: # Must specifiy a valid index_type
        if x not in ['HASHED', 'ASCENDING', 'DESCENDING']:
            print(f'ERROR! {x} is not a valid partioning strategy!')
            return
    if len(indexs) != len(index_types): # List containing indexs and list containing partioning strategy for each index must be same length
        print('ERROR! The same number of indexes and patrioning strategies must be specified across both lists!')
        return
    for i in range(len(indexs)):
        index_spec.append((indexs[i], index_type[index_types[i]]))
    with mongo_session() as conn:
        db = conn[f'{db_name}']
        collection = db[f'{collection_name}']
        collection.create_index(index_spec, unique=_unique)

def insert_document(db_name:str, collection_name:str, python_dicts:list):
    """Inserts a document into a specified collection"""
    with mongo_session() as conn:
        db = conn[f'{db_name}']
        collection = db[f'{collection_name}']
        result = collection.insert_many(python_dicts)
        print(f"Inserted {len(result.inserted_ids)} documents")
        print("Inserted IDs:", result.inserted_ids)
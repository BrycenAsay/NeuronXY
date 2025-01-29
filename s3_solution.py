from sql_helper import create_db_connection
from sqlalchemy import text

if __name__ == '__main__':
    yes = create_db_connection(text(f"SELECT * FROM s3 WHERE bucket_id = 5"), return_result=True)
    for row in yes:
        print(row)
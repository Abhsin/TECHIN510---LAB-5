import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# db_user = os.getenv('DB_USER')
# db_pw = os.getenv('DB_PASSWORD')
# db_host = os.getenv('DB_HOST')
# db_port = os.getenv('DB_PORT')
# db_name = os.getenv('DB_NAME')
# conn_str = f'postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}'

# def get_db_conn():
#     conn = psycopg2.connect(conn_str)
#     conn.autocommit = True
#     return conn

host = os.getenv('DB_HOST')
dbname = 'postgres'
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
sslmode = "require"
 
# Construct connection string
def get_db_conn():
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    #print(conn_string)
    conn = psycopg2.connect(conn_string)
    print("Connection established")
    conn.autocommit = True
    return conn
 
#get_db_conn()

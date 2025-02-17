import oracledb
from load_dotenv import env

conn = None

db_user = env["DB_USER"]
db_pass = env["DB_PASS"]
db_datasource = env["DB_DATASOURCE"]

def get_db_connection():

    global conn

    if conn:
        return conn

    try:
        conn = oracledb.connect(user=db_user,
                                password=db_pass,
                                dsn=db_datasource)
        print("[x] Database connection established!")
        return conn
    except Exception as e:
        print(f"[x] Database connection error: {e}")
        return None

def close_connection(conn):
    if conn:
        conn.close()
        print("[x] Database connection closed!")
    else:
        print("[x] No database connection to close!")

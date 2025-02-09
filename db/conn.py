import oracledb

def get_connection():
    conn = oracledb.connect(user="C##BOTFTP",
                            password="ftp",
                            dsn="192.168.0.13:1530/xe")
    print("✅ Connection open!")
    return conn

# def close_connection(conn):
#     if conn:
#         conn.close()
#         print("✅ Connection closed!")

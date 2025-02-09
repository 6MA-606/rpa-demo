import json
import paramiko
from robocorp.tasks import task
from sftp.conn import get_sftp_client
from rabbitmq.conn import set_rabbitmq_queue, publish_message
# from sftp.conn import get_sftp_client

# connect Oracle
from db.conn import get_connection 

paramiko.util.log_to_file("paramiko.log")

@task
def get_all_Documents():
    conn = get_connection()
    if conn is None:
        print("❌ ไม่สามารถเชื่อมต่อฐานข้อมูลได้")
        return []

    try:
        cursor = conn.cursor()
        query = "SELECT * FROM BOTFTP_TEST"
        cursor.execute(query)
        result = cursor.fetchall()

    except Exception as e:
        print(f"❌ Query failed: {e}")
        result = []

    finally:
        cursor.close()
        conn.close
        print("✅ Connection closed!")
    return result  

def loop_display(listDocs):
    for doc in listDocs:
        print(f"Document : {doc}")

def get_DocumentByname(docName):
    conn = get_connection()
    if conn is None:
        print("❌ ไม่สามารถเชื่อมต่อฐานข้อมูลได้")

    try:
        cursor = conn.cursor()
        query = f"SELECT CHECK_NAME_EXISTS('{docName}') AS result FROM dual"
        cursor.execute(query)
        result = cursor.fetchone()

    except Exception as e:
        print(f"❌ Query failed: {e}")
    finally:
        cursor.close()
        conn.close
        print("✅ Connection closed!")
        print(f"Result Search = 'Bot1' : {result}")   
        
@task
def enqueue_file(file_name):
    """Enqueue file to RabbitMQ"""
    set_rabbitmq_queue("file_queue")
    publish_message("file_queue", json.dumps({"file_name": file_name}))


if __name__ == "__main__":
    listDocs = get_all_Documents()
    loop_display(listDocs)
    get_DocumentByname('Bot1')

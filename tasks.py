import json
import os
import paramiko
from load_dotenv import env
from robocorp.tasks import task
from sftp.conn import get_sftp_client
from rabbitmq.conn import publish_message_to_exchange
from db.conn import get_db_connection 
from datetime import datetime

paramiko.util.log_to_file("paramiko.log")

sftp_remote_dir = env["SFTP_REMOTE_DIR"]
sftp_local_dir = env["SFTP_LOCAL_DIR"]

def pull_file(file_name, db_conn):
    """Pull file from SFTP"""

    global sftp_remote_dir
    global sftp_local_dir

    sftp_client = get_sftp_client(sftp_remote_dir)
    sftp_client.get(file_name, f"{sftp_local_dir}/{file_name}")

    cursor = db_conn.cursor()

    try:
        # TODO: Implement SEQUENCE in Oracle DB
        result = cursor.execute("SELECT COUNT(*) FROM PROCESS_HISTORY")
        seq_process = result.fetchone()[0] + 1
        cursor.execute("INSERT INTO PROCESS_HISTORY (SEQ_PROCESS, DOC_NAME, PROCESS_STATUS, CREATED_BY, CREATED_DATE) VALUES (:seq_process, :doc_name, '0', 'Schedule RPA', CURRENT_TIMESTAMP)", {
            "seq_process": seq_process,
            "doc_name": file_name,
        })
        db_conn.commit()
    except Exception as e:
        print(f"Error pull_file: {e}")
        db_conn.rollback()
        
    sftp_client.close()

def enqueue_file(file, db_conn):
    """Enqueue file to RabbitMQ"""
    publish_message_to_exchange(exchange_name="documents",
                                exchange_type="topic",
                                routing_key=f"documents.{file['bank_code']}.{file['service_code']}",
                                message=json.dumps({"file": file}))
    
    try:
        cursor = db_conn.cursor()
        # TODO: Implement SEQUENCE in Oracle DB
        result = cursor.execute("SELECT COUNT(*) FROM PROCESS_HISTORY")
        seq_process = result.fetchone()[0] + 1
        cursor.execute("INSERT INTO PROCESS_HISTORY (SEQ_PROCESS, DOC_NAME, PROCESS_STATUS, CREATED_BY, CREATED_DATE) VALUES (:seq_process, :doc_name, '0', 'Schedule RPA', CURRENT_TIMESTAMP)", {
            "seq_process": seq_process,
            "doc_name": file["doc_name"],
        })
        db_conn.commit()
    except Exception as e:
        print(f"Error enqueue_file: {e}")
        db_conn.rollback()

@task
def main():

    global sftp_remote_dir
    global sftp_local_dir

    if not os.path.exists(sftp_local_dir):
        os.mkdir(sftp_local_dir)

    sftp_client = get_sftp_client(sftp_remote_dir)
    sftp_files = sftp_client.listdir()

    if not sftp_files:
        print("No files to process")
        sftp_client.close()
        return

    db_conn = get_db_connection()

    # File name format is: "B###_S###_YYYYMMDD.txt"
    for file_name in sftp_files:
        if file_name.endswith(".txt"):
            cursor = db_conn.cursor()
            result = cursor.execute("SELECT COUNT(*) FROM DOCUMENTS_INFO WHERE DOC_NAME = :doc_name", {"doc_name": file_name})
            isProcessed = result.fetchone()[0] > 0

            if not isProcessed:

                bank_code, service_code, doc_date = file_name.split(".")[0].split("_")

                file = {
                    "bank_code": bank_code,
                    "service_code": service_code,
                    "doc_date": doc_date,
                    "doc_name": file_name
                }

                pull_file(file_name, db_conn)
                enqueue_file(file, db_conn)
                print(f"Enqueued file {file_name}")

            else:
                print(f"File {file_name} already processed")

    db_conn.close()
    sftp_client.close()

if __name__ == "__main__":
    main()
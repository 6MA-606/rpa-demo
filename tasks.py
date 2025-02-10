import json
import os
import paramiko
from load_dotenv import env
from robocorp.tasks import task
from sftp.conn import get_sftp_client
from rabbitmq.conn import publish_message_to_exchange
from db.conn import get_db_connection 
from datetime import datetime
from libs.logManagement import write_log
from libs.status import Status
# import sys
# import io

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

paramiko.util.log_to_file("paramiko.log")

sftp_remote_dir = env["SFTP_REMOTE_DIR"]
sftp_local_dir = env["SFTP_LOCAL_DIR"]

def pull_file(file_name):
    """Pull file from SFTP"""

    global sftp_remote_dir
    global sftp_local_dir

    try:
        sftp_client = get_sftp_client(sftp_remote_dir)
        sftp_client.get(file_name, f"{sftp_local_dir}/{file_name}", callback=write_log(process_name="pull_sftp_client_file",
                                                                                       doc_name=file_name,
                                                                                       process_status=Status.INPROGRESS.value,
                                                                                       annotation="File pulling from SFTP",
                                                                                       create_by="Schedule RPA"))

        write_log(process_name="pull_sftp_client_file",
                  doc_name=file_name,
                  process_status=Status.SUCCESS.value,
                  annotation="File pulled from SFTP",
                  create_by="Schedule RPA")
    except Exception as e:
        print(f"Error pull_file: {e}")

        write_log(process_name="pull_sftp_client_file",
                  doc_name=file_name,
                  process_status=Status.FAILED.value,
                  annotation="Error pulling file from SFTP",
                  create_by="Schedule RPA")
        
    sftp_client.close()

def enqueue_file(file):
    """Enqueue file to RabbitMQ"""

    try:
        publish_message_to_exchange(exchange_name="documents",
                                    exchange_type="topic",
                                    routing_key=f"documents.{file['bank_code']}.{file['service_code']}",
                                    message=json.dumps({"file": file}))
    
        write_log(process_name="enqueue_file",
                  doc_name=file["doc_name"],
                  process_status=Status.SUCCESS.value,
                  annotation="File enqueued",
                  create_by="Schedule RPA")
    except Exception as e:
        print(f"Error enqueue_file: {e}")

        write_log(process_name="enqueue_file",
                  doc_name=file["doc_name"],
                  process_status=Status.FAILED.value,
                  annotation="Error enqueuing file",
                  create_by="Schedule RPA")


@task
def main():

    global sftp_remote_dir
    global sftp_local_dir

    if not os.path.exists(sftp_local_dir):
        os.mkdir(sftp_local_dir)

    sftp_client = get_sftp_client(sftp_remote_dir)
    sftp_files = sftp_client.listdir()

    if not sftp_files:
        print("[!] No files to process")
        sftp_client.close()
        return

    db_conn = get_db_connection()

    # File name format is: "B###_S###_YYYYMMDD.txt"
    for file_name in sftp_files:
        if file_name.endswith(".txt"):
            cursor = db_conn.cursor()
            result = cursor.execute("""
                                        SELECT CASE WHEN EXISTS (SELECT 1 FROM DOCUMENTS_INFO WHERE DOC_NAME = :doc_name) THEN 1 ELSE 0 END
                                        FROM dual
                                    """, {"doc_name": file_name}) 
            isProcessed = result.fetchone()[0] == 1

            if not isProcessed:

                bank_code, service_code, doc_date = file_name.split(".")[0].split("_")

                file = {
                    "bank_code": bank_code,
                    "service_code": service_code,
                    "doc_date": doc_date,
                    "doc_name": file_name
                }

                pull_file(file_name)
                enqueue_file(file)
                print(f"[o] Enqueued file {file_name}")

            else:
                print(f"[!] File {file_name} already processed")

    db_conn.close()
    sftp_client.close()

if __name__ == "__main__":
    main()
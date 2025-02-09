import pika
import json
import os
import paramiko
from load_dotenv import env
from robocorp.tasks import task
from sftp.conn import get_sftp_client
from rabbitmq.conn import publish_message_to_exchange
from db.conn import get_db_connection 
from datetime import datetime

rabbitmq_host = env["RABBITMQ_HOST"]
rabbitmq_port = env["RABBITMQ_PORT"]

sftp_local_dir = env["SFTP_LOCAL_DIR"]

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

channel = connection.channel()

channel.exchange_declare(exchange="documents", exchange_type="topic")

result = channel.queue_declare(queue='', exclusive=True)
quene_name = result.method.queue

channel.queue_bind(exchange="documents", queue=quene_name, routing_key="documents.#")

def save_to_db(file, db_conn):
    cursor = db_conn.cursor()

    file_content = None

    # Read file content
    with open(f"{sftp_local_dir}/{file['doc_name']}", "r") as f:
        file_content = f.read()

    try:
        cursor.execute("SELECT COUNT(*) FROM DOCUMENTS_INFO")
        seq = cursor.fetchone()[0] + 1
        cursor.execute("INSERT INTO DOCUMENTS_INFO (SEQ, BANK_CODE, SERVICE_CODE, DOC_NAME, CONTENT, CREATED_DATE, UPDATED_DATE) VALUES (:seq, :bank_code, :service_code, :doc_name, :content, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", {
            "seq": seq,
            "bank_code": file["bank_code"],
            "service_code": file["service_code"],
            "doc_name": file["doc_name"],
            "content": file_content
        })
        db_conn.commit()
    except Exception as e:
        print(f"Error save_to_db: {e}")
        db_conn.rollback()


def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

    file = json.loads(body)
    db_conn = get_db_connection()

    save_to_db(file, db_conn)

    cursor = db_conn.cursor()
    # TODO: Implement SEQUENCE in Oracle DB
    result = cursor.execute("SELECT COUNT(*) FROM PROCESS_HISTORY")
    seq_process = result.fetchone()[0] + 1
    cursor.execute("INSERT INTO PROCESS_HISTORY (SEQ_PROCESS, DOC_NAME, PROCESS_STATUS, CREATED_BY, CREATED_DATE) VALUES (:seq_process, :doc_name, '2', 'Schedule RPA', CURRENT_TIMESTAMP)", {
        "seq_process": seq_process,
        "doc_name": file["doc_name"],
    })

channel.basic_consume(queue=quene_name, on_message_callback=callback, auto_ack=True)

print(" [*] Waiting for messages. To exit press CTRL+C")

channel.start_consuming()
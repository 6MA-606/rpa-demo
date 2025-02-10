import pika
import json
from load_dotenv import env
from libs.logManagement import write_log
from libs.status import Status
from db.conn import get_db_connection

rabbitmq_host = env["RABBITMQ_HOST"]
rabbitmq_port = env["RABBITMQ_PORT"]

sftp_local_dir = env["SFTP_LOCAL_DIR"]

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

channel = connection.channel()

channel.exchange_declare(exchange="documents", exchange_type="topic", durable=True)

result = channel.queue_declare(queue='', exclusive=True, durable=True)
quene_name = result.method.queue

channel.queue_bind(exchange="documents", queue=quene_name, routing_key="documents.#")

def save_to_db(file):

    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    file_content = None

    # Read file content
    with open(f"{sftp_local_dir}/{file['doc_name']}", "r") as f:
        file_content = f.read()

    try:
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.INPROGRESS.value,
                  annotation="Saving file content to database",
                  create_by="File Content Extractor Service")
        
        cursor.execute("INSERT INTO DOCUMENTS_INFO (BANK_CODE, SERVICE_CODE, DOC_NAME, CONTENT, CREATED_DATE, UPDATED_DATE) VALUES (:bank_code, :service_code, :doc_name, :content, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", {
            "bank_code": file["bank_code"],
            "service_code": file["service_code"],
            "doc_name": file["doc_name"],
            "content": file_content
        })
        db_conn.commit()
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.SUCCESS.value,
                  annotation="File content saved to database",
                  create_by="File Content Extractor Service")
    except Exception as e:
        print(f"Error save_to_db: {e}")
        db_conn.rollback()
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.FAILED.value,
                  annotation="Error saving file content to database",
                  create_by="File Content Extractor Service")


def callback(ch, method, properties, body):
    print(f" [x] Received message: {json.loads(body)['file']['doc_name']}")

    file = json.loads(body)["file"]

    save_to_db(file)

    ch.basic_qos(prefetch_count=1)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue=quene_name, on_message_callback=callback)

print(" [*] Waiting for messages. To exit press CTRL+C")

channel.start_consuming()
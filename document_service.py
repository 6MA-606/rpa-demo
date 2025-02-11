import pika
import json
from load_dotenv import env
from libs.logManagement import write_log
from libs.documentInfoManagement import create_document_info
from libs.status import Status

rabbitmq_host = env["RABBITMQ_HOST"]
rabbitmq_port = env["RABBITMQ_PORT"]

sftp_local_dir = env["SFTP_LOCAL_DIR"]

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))

channel = connection.channel()

channel.exchange_declare(exchange="documents", exchange_type="topic", durable=True)

args = {
    'x-message-ttl': 60000,  # TTL 60 วินาที
    'x-dead-letter-exchange': 'documents',
    'x-dead-letter-routing-key': 'documents.#',
}

quene_name = 'documents_service'
result = channel.queue_declare(queue=quene_name, durable=True, auto_delete=True, arguments=args)

channel.queue_bind(exchange="documents", queue=quene_name, routing_key="documents.#")

def save_to_db(file):

    file_content = None

    # Read file content
    with open(f"{sftp_local_dir}/{file['doc_name']}", "r", encoding="utf-8") as f:
        file_content = f.read()

    try:
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.INPROGRESS.value,
                  annotation="Saving file content to database",
                  create_by="File Content Extractor Service")
        
        create_document_info(bank_code=file["bank_code"],
                             service_code=file["service_code"],
                             doc_name=file["doc_name"],
                             doc_issue_date=file["doc_date"],
                             content=file_content)
        
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.SUCCESS.value,
                  annotation="File content saved to database",
                  create_by="File Content Extractor Service")
    except Exception as e:
        print(f"Error save_to_db: {e}")
        write_log(process_name="save_file_content_to_db",
                  doc_name=file["doc_name"],
                  process_status=Status.FAILED.value,
                  annotation="Error saving file content to database",
                  create_by="File Content Extractor Service")
        raise e


def callback(ch, method, properties, body):
    print(f" [x] Received message: {json.loads(body)['file']['doc_name']}")

    file = json.loads(body)["file"]

    try:
        save_to_db(file)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error while saving file content to database: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=quene_name, on_message_callback=callback)

print(" [*] Waiting for messages. To exit press CTRL+C")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print(" [*] Consumer interrupted, stopping.")
    channel.stop_consuming()

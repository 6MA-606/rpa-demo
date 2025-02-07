import json
import paramiko
from robocorp.tasks import task
from sftp.conn import get_sftp_client
from rabbitmq.conn import set_rabbitmq_queue, publish_message

paramiko.util.log_to_file("paramiko.log")

@task
def list_sftp_file():
    """List files in SFTP directory"""
    sftp_client = get_sftp_client("/uploads")
    print(sftp_client.listdir())

@task
def enqueue_file(file_name):
    """Enqueue file to RabbitMQ"""
    set_rabbitmq_queue("file_queue")
    publish_message("file_queue", json.dumps({"file_name": file_name}))
    

def main():
    enqueue_file("file1.txt")

if __name__ == "__main__":
    main()

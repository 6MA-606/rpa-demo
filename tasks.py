import json
import paramiko
from robocorp.tasks import task
from sftp.conn import get_sftp_client

paramiko.util.log_to_file("paramiko.log")

@task
def list_sftp_file():
    """List files in SFTP directory"""
    sftp_client = get_sftp_client("/uploads")
    print(sftp_client.listdir())


if __name__ == "__main__":
    list_sftp_file()  

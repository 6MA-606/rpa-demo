import paramiko
from load_dotenv import env

paramiko.util.log_to_file("./sftp/paramiko.log")

sftp_host = env["SFTP_HOST"]
sftp_port = env["SFTP_PORT"]
sftp_user = env["SFTP_USER"]
sftp_pass = env["SFTP_PASS"]

def get_sftp_client(sftp_dir):
    transport = paramiko.Transport((sftp_host, int(sftp_port)))
    transport.connect(username=sftp_user, password=sftp_pass)
    sftp = paramiko.SFTPClient.from_transport(transport)

    if sftp_dir:
        sftp.chdir(sftp_dir)

    return sftp
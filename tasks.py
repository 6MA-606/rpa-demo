import os
import json
from RPA.Robocorp.WorkItems import WorkItems
from RPA.FileSystem import FileSystem
from RPA.JSON import JSON
from robocorp.tasks import task

fs = FileSystem()
input_dir = "./file_server"
queue = "./queue"

@task
def read_files_and_print_name():
    """Read files from the file system and print their names."""
    for file in fs.list_files_in_directory(input_dir):
        file_name = file[1]
        service_name, timestamp, _ = file_name.split("-")

        data = {
            "service_name": service_name,
            "timestamp": timestamp,
            "file_path": file[0]
        }

        try:
            if fs.does_directory_not_exist(queue):
                fs.create_directory(queue)
        except Exception as e:
            print(f"Error: {e}")

        json_str = json.dumps(data, indent=4)

        new_file_name = f"{service_name}-{timestamp}.json"
        if fs.file_exists(f"{queue}/{new_file_name}"):
            print(f"File already exists: {new_file_name}")
        else:
            fs.create_file(f"{queue}/{new_file_name}", json_str)
            print(f"File created: {new_file_name}")



    

from enum import Enum

class Status(Enum):
    READY = "0"
    QUEUE = "1"
    SUCCESS = "2"
    INPROGRESS = "3"
    FAILED = "4"

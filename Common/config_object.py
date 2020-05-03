from Common.helper import json_file_to_object


class Config:
    def __init__(self, config_file):
        self.__dict__ = json_file_to_object(config_file)

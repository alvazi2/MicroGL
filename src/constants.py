import json

class Constants:
    constants_file_path: str
    config: dict

    def __init__(self, constants_file_path: str):
        self.constants_file_path = constants_file_path
        with open(constants_file_path, 'r') as file:
            self.config = json.load(file)
    
    def get(self, key: str):
        if key not in self.config:
            raise KeyError(f"Key '{key}' not found in configuration file {self.constants_file_path}.")
        return self.config[key]

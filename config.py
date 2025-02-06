import json

class Config:
    config_file_path: str
    config: dict

    def __init__(self, config_file_path: str):
        self.config_file_path = config_file_path
        with open(config_file_path, 'r') as file:
            self.config = json.load(file)
    
    def get(self, key: str):
        if key not in self.config:
            raise KeyError(f"Key '{key}' not found in configuration file {self.config_file_path}.")
        return self.config[key]

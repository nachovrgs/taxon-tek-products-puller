import json

from src.settings import Config


class ConfigConnector:
    """
    Handles the communication to local config file
    """

    def get_config(self):
        with open(Config.CONFIG_FILE) as config_file:
            json_content = json.load(config_file)
            return json_content

    def set_config(self, config):
        with open(Config.CONFIG_FILE, "w") as f:
            json.dump(config, f)

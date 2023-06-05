import os
from io import BytesIO
import pandas as pd
import requests
from src.logging.logger import get_module_logger

logger = get_module_logger(__name__)


class FileConnector:
    """
    Handles the communication to local files
    """

    def __init__(self):
        self.tmp_folder = "tmp"
        is_exist = os.path.exists(self.tmp_folder)
        if not is_exist:
            os.makedirs(self.tmp_folder)

    def read_and_write_file_locally(self, source_url, filename, encoding):
        req = requests.get(source_url)
        url_content = req.content
        loaded = False
        try:
            pd.read_csv(BytesIO(url_content), encoding=encoding, sep=";")
            loaded = True
        except Exception as loading_error:
            logger.error(
                f"Could not load response file from {source_url}. \n"
                f"Error: {loading_error}"
            )
        if loaded:
            with open(f"{self.tmp_folder}/{filename}", "wb") as file:
                file.write(url_content)
                file.close()

    def get_file_df(self, filename, encoding, separator=";"):
        try:
            return pd.read_csv(
                f"{self.tmp_folder}/{filename}", encoding=encoding, sep=separator
            )
        except FileNotFoundError as not_found:
            logger.warn(f"Source {filename} was not found locally. Moving on.")
            return None

    def delete_local_file(self, filename):
        try:
            os.remove(f"{self.tmp_folder}/{filename}")
        except OSError:
            pass

import os
import traceback
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
from pysftp import CnOpts, Connection

from src.logging.logger import get_module_logger

logger = get_module_logger(__name__)


class FileConnector:
    """
    Handles the communication to local files with FTP
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

    def read_and_write_ftp_locally(
        self, source_url, file_path, filename, expected_file, user, password
    ):
        cnopts = CnOpts()
        cnopts.hostkeys = None

        try:
            extension = file_path.split(".")[-1]
            local_raw_path = f"{self.tmp_folder}/{filename}.{extension}"
            with Connection(
                source_url, username=user, password=password, cnopts=cnopts
            ) as sftp:
                sftp.get(remotepath=file_path, localpath=local_raw_path)
            if extension.lower() == "zip":
                with ZipFile(local_raw_path, "r") as zObject:
                    zObject.extractall(path=self.tmp_folder)
                local_expected_path = f"{self.tmp_folder}/{expected_file}"
                local_final_path = f"{self.tmp_folder}/{filename}.csv"
                os.rename(local_expected_path, local_final_path)
                if os.path.exists(local_raw_path):
                    os.remove(local_raw_path)

        except Exception as error:
            tb = traceback.format_exc()
            error_message = (
                f"Unexpected error while downloading ftp data. \n"
                f"Error: {error}. \n"
                f"Traceback: {tb}"
            )
            logger.error(msg=error_message)

    def get_file_df(
        self,
        filename,
        encoding,
        separator=";",
        header=None,
        names=None,
        engine="python",
    ):
        try:
            return pd.read_csv(
                f"{self.tmp_folder}/{filename}",
                encoding=encoding,
                sep=separator,
                header=header,
                names=names,
                engine=engine,
            )
        except FileNotFoundError as not_found:
            logger.warn(f"Source {filename} was not found locally. Moving on.")
            return None

    def delete_local_file(self, filename):
        try:
            os.remove(f"{self.tmp_folder}/{filename}")
        except OSError:
            pass

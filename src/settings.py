# -*- coding: utf-8 -*-
"""Application configuration."""
import os
import pathlib

from dotenv import load_dotenv

env_path = f"{pathlib.Path(__file__).parent.parent.resolve()}/.env"  # pylint:disable=invalid-name

load_dotenv(env_path)


class Config:
    """Base configuration which reads in variables through
    the environment which has loaded them from .env"""

    ENV = os.getenv("ENV")

    LOG_LEVEL = os.getenv("LOG_LEVEL")
    LOG_FILE_NAME = os.getenv("LOG_FILE_NAME")

    DATE_FORMAT = os.getenv("DATE_FORMAT")

    CONFIG_FILE = os.getenv("CONFIG_FILE")

    FTP_HOSTNAME = os.getenv("FTP_HOSTNAME")
    FTP_USERNAME = os.getenv("FTP_USERNAME")
    FTP_PASSWORD = os.getenv("FTP_PASSWORD")

    WCAPI_URL = os.getenv("WCAPI_URL")
    ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
    ACCESS_KEY = os.getenv("ACCESS_KEY")

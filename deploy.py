import os
import os.path
from ftplib import FTP, error_perm

from src.settings import Config

IGNORE = [
    ".idea",
    ".git",
    ".github",
    ".gitignore",
    "deploy.py",
    "Makefile",
    "README.md",
    "config.json",
    "requirements.local.txt",
]


def store_file(session, path):
    for name in os.listdir(path):
        if name not in IGNORE:
            localpath = os.path.join(path, name)
            if os.path.isfile(localpath):
                with open(localpath, "rb") as file:
                    session.storbinary("STOR {}".format(name), file)
            elif os.path.isdir(localpath):
                try:
                    session.mkd(name)
                # ignore "directory already exists"
                except error_perm as e:
                    if not e.args[0].startswith("550"):
                        raise
                session.cwd(name)
                store_file(session, localpath)
                session.cwd("..")


if __name__ == "__main__":
    with FTP(Config.FTP_HOSTNAME, Config.FTP_USERNAME, Config.FTP_PASSWORD) as session:
        session.encoding = "utf-8"
        store_file(session=session, path=".")

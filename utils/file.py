import aiofiles
import aiofiles.os
from pathlib import Path
import logging
import os


def make_folder(folder_path):
    """ Synchronous implementation of make file"""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        logging.info("%s created successfully", folder_path)
    else:
        logging.info("%s already exists", folder_path)

# -*- coding: utf-8 -*-
"""Logging module"""
import logging
import os
from datetime import datetime as dt

from pgwasdbi import __version__


def configure(module_name, *args, **kwargs):
    """Set up log files and associated handlers"""
    # Configure logging, stderr and file logs
    logging_level = logging.INFO
    if "verbose" in kwargs and kwargs["verbose"]:
        logging_level = logging.DEBUG


    logFormatter = logging.Formatter("%(asctime)s - [%(levelname)-4.8s] - %(filename)s %(lineno)d - %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging_level)
    rootLogger.addHandler(consoleHandler)

    # log filepath
    lfp = f"{dt.today().strftime('%Y-%m-%d_%H:%M:%S')}-{os.path.splitext(__loader__.name)[0]}.log"

    logFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s %(lineno)d - %(message)s')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging_level)

    fileHandler = logging.FileHandler(lfp)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    logging.debug(f'Running {module_name} {__version__}')
    logging.debug(f"{args=}")
    logging.debug(f"{kwargs=}")

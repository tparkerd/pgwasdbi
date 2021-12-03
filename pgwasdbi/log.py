# -*- coding: utf-8 -*-
"""Logging module"""
import logging
import os
from datetime import datetime as dt

from appdirs import user_config_dir, user_log_dir
from pgwasdbi.settings import appauthor, appname

from pgwasdbi import __version__


def configure(module_name, *args, **kwargs):
    """Set up log files and associated handlers"""
    # Configure logging, stderr and file logs
    logging_level = logging.INFO
    if "verbose" in kwargs and kwargs["verbose"]:
        logging_level = logging.DEBUG
        
    if 'path' in kwargs:
        path = kwargs['path']
    else:
        path = None

    logFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s %(lineno)d - %(message)s')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging_level)
    
    # Set project-level logging
    if module_name is not None:
        logfile_basename = f"{dt.today().strftime('%Y-%m-%d_%H-%M-%S')}_{module_name}.log"
    # If the input path is a directory, place log into it
    if path is not None and os.path.isdir(path[0].name):
        lfp = os.path.join(os.path.realpath(path[0].name), logfile_basename)
    # Otherwise, put the log file into the folder of the selected file
    else:
        log_dir = user_log_dir(appname=appname, appauthor=appauthor)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        lfp = os.path.join(log_dir, logfile_basename) # base log file path

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging_level)
    rootLogger.addHandler(consoleHandler)

    fileHandler = logging.FileHandler(lfp)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    logging.debug(f'Running {module_name} {__version__}')
    logging.debug(f"{args=}")
    logging.debug(f"{kwargs=}")
    logging.info(f"Logging to '{lfp}'")

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

    __version__ = version('pgwasdbi')

    logFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s %(lineno)d - %(message)s')
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging_level)
    
    # Set project-level logging
    if args.module_name is not None:
        logfile_basename = f"{dt.today().strftime('%Y-%m-%d_%H-%M-%S')}_{args.module_name}.log"
    # If the input path is a directory, place log into it
    if os.path.isdir(args.path[0].name):
        lfp = os.path.join(os.path.realpath(args.path[0].name), logfile_basename)
    # Otherwise, put the log file into the folder of the selected file
    else:
        lfp = os.path.join(os.path.realpath(os.path.dirname(args.path[0].name)), logfile_basename) # base log file path

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

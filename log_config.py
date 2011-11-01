#!/usr/bin/env python
# encoding: utf-8
"""
log_config.py

Created by Brian Lalor on 2011-11-01.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys
import os

import logging, logging.handlers

def init_logging_stdout(level = logging.DEBUG):
    handler = logging.StreamHandler(stream = sys.stdout)
    
    config_root_logger(handler, level)

    
def init_logging(logfile, level = logging.INFO):
    handler = logging.handlers.RotatingFileHandler(logfile,
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    config_root_logger(handler, level)


def config_root_logger(handler, level):
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(threadName)s] %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(level)



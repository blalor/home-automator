#!/usr/bin/env python
# encoding: utf-8
"""
sprinkler_controller.py

Created by Brian Lalor on 2011-05-15.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import xmlrpclib

from config import config_data as config

def main():
    srv = xmlrpclib.Server(
        "http://%s:%d" % (config.xmlrpc_server.host, config.xmlrpc_server.port)
    )
    
    meth = getattr(srv, 'sprinkler.' + sys.argv[1])
    meth(int(sys.argv[2]))
    


if __name__ == '__main__':
    main()


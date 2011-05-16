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

def main():
    srv = xmlrpclib.Server("http://pepe:10103")
    
    meth = getattr(srv, sys.argv[1])
    meth(int(sys.argv[2]))
    


if __name__ == '__main__':
    main()


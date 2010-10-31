#!/usr/bin/env python
# encoding: utf-8
"""
XBeeProxy.py

Created by Brian Lalor on 2010-10-29.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import unittest

from xbee import XBee
import cPickle as pickle
import socket
import struct

class PeerDiedError(Exception):
    pass

# {{{{ XBeeProxy
class XBeeProxy(XBee):
    # {{{ __init__
    def __init__(self, socket, shorthand=True, callback=None):
        self.socket = socket
        super(XBee, self).__init__(None, shorthand, callback)
    
    # }}}
    
    # {{{ send
    def send(self, name, **kwargs):
        self.socket.settimeout(None)
        
        data = pickle.dumps((name, kwargs))
        self.socket.send(struct.pack('I', len(data)))
        self.socket.sendall(data)
    
    # }}}
    
    # {{{ wait_read_frame
    def wait_read_frame(self):
        # enable short timeout for receiving data
        self.socket.settimeout(0.25)
        header_len = struct.calcsize('I')
        
        while True:
            try:
                header_dat = self.socket.recv(header_len)
                if len(header_dat) == 0:
                    raise PeerDiedError()
            
                data_len = struct.unpack('I', header_dat)[0]
                packet = pickle.loads(self.socket.recv(data_len))
                return packet
            except socket.timeout:
                # print 'timeout reading data'
                pass
            
        
    
    # }}}
    
# }}}}


class XBeeProxyTests(unittest.TestCase):
    def setUp(self):
        self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.s.connect('foo')
        self.proxyUnderTest = XBeeProxy(self.s)
        
    
    
    def tearDown(self):
        self.s.close()
        del self.s
        del self.proxyUnderTest
    
    def testOne(self):
        self.proxyUnderTest.send('at', bar='baz')
    
    def testFail(self):
        self.assertRaises(AttributeError, lambda: self.proxyUnderTest.qwerty('at', bar='baz'))
    
    


if __name__ == '__main__':
    import socket
    
    unittest.main()

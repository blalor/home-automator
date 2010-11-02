#!/usr/bin/env python
# encoding: utf-8
"""
XBeeProxy.py

Created by Brian Lalor on 2010-10-29.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

from __future__ import with_statement

import sys
import os
import unittest

from xbee import XBee
import cPickle as pickle
import socket
import struct
import threading
import logging

class PeerDiedError(Exception):
    pass

# {{{{ XBeeProxy
class XBeeProxy(XBee):
    # {{{ __init__
    def __init__(self, _socket, shorthand=True, callback=None):
        self._logger = logging.getLogger(self.__class__.__name__)
        
        # probably wouldn't need the lock for writing *and* reading, except 
        # that both set the timeout…
        self.__socket_send_lock = threading.RLock()
        self.__socket_recv_lock = threading.RLock()
        
        self.socket = _socket
        
        # set receive timeout to 0.25 seconds
        # http://bugs.python.org/file827/getsockopt_test.py
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, struct.pack('LL', 0, 250000))
        
        super(XBee, self).__init__(None, shorthand, callback)
    
    # }}}
    
    # {{{ send
    def send(self, name, **kwargs):
        with self.__socket_send_lock:
            # self.socket.settimeout(None)
            
            data = pickle.dumps((name, kwargs), pickle.HIGHEST_PROTOCOL)
            self.socket.sendall(struct.pack('!I', len(data)))
            self.socket.sendall(data)
    
    # }}}
    
    # {{{ wait_read_frame
    def wait_read_frame(self):
        with self.__socket_recv_lock:
            # enable short timeout for receiving data
            # self.socket.settimeout(0.25)
            header_len = struct.calcsize('!I')
            
            while True:
                try:
                    header_dat = self.socket.recv(header_len, socket.MSG_WAITALL)
                    if len(header_dat) == 0:
                        raise PeerDiedError()
                    
                    data_len = struct.unpack('!I', header_dat)[0]
                    data = self.socket.recv(data_len, socket.MSG_WAITALL)
                    
                    return pickle.loads(data)
                except socket.timeout:
                    # timeout if using socket.settimeout()
                    # print 'timeout reading data'
                    pass
                except socket.error:
                    # timeout if using SO_RCVTIMEO and MSG_WAITALL
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

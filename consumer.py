#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import socket

import struct
import XBeeProxy

import datetime

import logging
import logging.handlers

class Disconnected(Exception):
    pass


class BaseConsumer(object):
    __default_socket_path = ('localhost', 9999)
    __frame_id = '\x01'
    
    # {{{ __init__
    def __init__(self, xbee_addresses = []):
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.xbee_addresses = [self._parse_addr(xba) for xba in xbee_addresses]
        
        self.__shutdown = False
        
        # self.__socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        ## this may only apply to TCP sockets...
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        self.__socket.connect(self.__default_socket_path)
        self._logger.info("connected to %s", self.__default_socket_path)
        
        self.xbee = XBeeProxy.XBeeProxy(self.__socket)
    # }}}
    
    # {{{ _parse_addr
    def _parse_addr(self, addr):
        paddr = None
        
        if addr != None:
            paddr = "".join(chr(int(x, 16)) for x in addr.split(":"))
        
        return paddr
    
    # }}}
    
    # {{{ _format_addr
    def _format_addr(self, addr):
        return ":".join(['%02x' % ord(x) for x in addr])
    
    # }}}
    
    # {{{ _sample_to_mv
    def _sample_to_mv(self, sample):
        """Converts a raw A/D sample to mV (uncalibrated)."""
        return sample * 1200.0 / 1023
    # }}}
    
    # {{{ get_frame_id
    def get_frame_id(self):
        return self.__frame_id
    
    # }}}
    
    # {{{ next_frame_id
    def next_frame_id(self):
        self.__frame_id = chr(ord(self.__frame_id) + 1)
        
        return self.get_frame_id()
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self.__shutdown = True
        self.__socket.shutdown(socket.SHUT_RDWR)
        self.__socket.close()
        
        self._logger.info("shutdown complete")
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        while not self.__shutdown:
            try:
                frame = self.xbee.wait_read_frame()
                
                src_addr = None
                if 'source_addr_long' in frame:
                    src_addr = frame['source_addr_long']
                
                _do_process = True
                if self.xbee_addresses:
                    if (src_addr != None) and (src_addr not in self.xbee_addresses):
                        _do_process = False
                
                if _do_process:
                    self.handle_packet(frame)
                else:
                    # no match on address; filtered.
                    pass
            except:
                self._logger.critical("exception handling packet", exc_info = True)
        
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, packet):
        ## for testing only; subclasses should override
        self._logger.debug(unicode(str(packet), errors='replace'))
    # }}}
    
    # {{{ now
    def now(self):
        return datetime.datetime.now()
    # }}}
    


class DatabaseConsumer(BaseConsumer):
    # {{{ __init__
    def __init__(self, db_name, xbee_addresses = []):
        import sqlite3
        
        self.dbc = sqlite3.connect(db_name,
                                   isolation_level = None,
                                   timeout = 5)
        
        BaseConsumer.__init__(self, xbee_addresses)
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        BaseConsumer.process_forever(self)
        self.dbc.close()
    # }}}
    


if __name__ == '__main__':
    handler = logging.handlers.RotatingFileHandler('logs/consumer.log',
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        BaseConsumer().process_forever()
    finally:
        BaseConsumer().shutdown()
        logging.shutdown()


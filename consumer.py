#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import socket

import struct
import xbee

import datetime

class Disconnected(Exception):
    pass


class BaseConsumer(object):
    __default_socket_path = "socket"
    __header_pack_fmt = "BH"
    
    # {{{ __init__
    def __init__(self, xbee_address = None):
        self.xbee_address = xbee_address
        self.__shutdown = False
        self.__header_len = struct.calcsize(self.__header_pack_fmt)
        
        self.__socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        
        ## this may only apply to TCP sockets...
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        self.__socket.connect(self.__default_socket_path)
    # }}}
    
    
    # {{{ __find_packet
    def __find_packet(self):
        packet = None
        
        header = self.__socket.recv(self.__header_len)
        
        if len(header) != self.__header_len:
            raise Disconnected("expected %d bytes, got %d" % (self.__header_len, len(header)))
            
        else:
            packet_id, length = struct.unpack(self.__header_pack_fmt, header)
            
            if packet_id == xbee.xbee.START_IOPACKET:
                packet = xbee.xbee(self.__socket.recv(length))
        
        return packet
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self.__shutdown = True
    # }}}
    
    # {{{ process_packet
    def process_packet(self):
        packet = self.__find_packet()
        
        if packet:
            if (packet.address_64 == self.xbee_address) or (self.xbee_address == None):
                self.handle_packet(packet)
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        while not self.__shutdown:
            self.process_packet()
        
        self.__socket.close()
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, packet):
        ## for testing only; subclasses should override
        print packet
    # }}}
    
    # {{{ utcnow
    def utcnow(self):
        return datetime.datetime.utcnow()
    # }}}
    


class DatabaseConsumer(BaseConsumer):
    # {{{ __init__
    def __init__(self, db_name, xbee_address = None):
        import sqlite3
        
        self.dbc = sqlite3.connect(db_name,
                                   isolation_level = None,
                                   timeout = 5)
        
        BaseConsumer.__init__(self, xbee_address)
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        BaseConsumer.process_forever(self)
        self.dbc.close()
    # }}}
    


if __name__ == '__main__':
    BaseConsumer().process_forever()


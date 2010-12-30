#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys,os
import socket

import struct
import XBeeProxy

import datetime

import logging
import logging.handlers
import daemonizer
import Queue
import random

class Disconnected(Exception):
    pass


class InvalidDestination(Exception):
    pass


class BaseConsumer(object):
    # {{{ __init__
    def __init__(self, xbee_addresses = [], socket_dest = ('localhost', 9999)):
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.__frame_id = chr(((random.randint(1, 255)) % 255) + 1)
        self.__shutdown = False
        
        # queue for status frames that aren't explicitly handled in handle_packet,
        # so that __send_data can get to them.
        self.__status_msg_queue = Queue.Queue()
        
        self.xbee_addresses = [self._parse_addr(xba) for xba in xbee_addresses]
        
        # self.__socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        ## this may only apply to TCP sockets...
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        self.__socket.connect(socket_dest)
        self._logger.info("connected to %s", socket_dest)
        
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
        # increment frame_id but constrain to 1..255.  Seems to go
        # 2,4,6,…254,1,3,5…255. Whatever.
        self.__frame_id = chr(((ord(self.__frame_id) + 1) % 255) + 1)
        
        return self.get_frame_id()
    
    # }}}
    
    # {{{ _send_data
    def _send_data(self, dest, data):
        if dest not in self.xbee_addresses:
            raise InvalidDestination("destination address %s is not configured for this consumer" % self._format_addr(dest))
        
        success = False
        
        frame_id = self.next_frame_id()
        
        self.xbee.zb_tx_request(frame_id = frame_id, dest_addr_long = dest, data = data)
        
        while True:
            try:
                frame = self.__status_msg_queue.get(True, 1)
                # frame is guaranteed to have id == zb_tx_status
                
                # print frame
                if (frame['frame_id'] == frame_id):
                    if frame['delivery_status'] == '\x00':
                        # success!
                        success = True
                        self._logger.debug("sent data with %d retries", ord(frame['retries']))
                    else:
                        self._logger.error("send failed after %d retries with status 0x%2X", ord(frame['retries']), ord(frame['delivery_status']))
                    
                    break
            except Queue.Empty:
                pass
            
        return success
    
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
                    if not self.handle_packet(frame):
                        if (frame['id'] == 'zb_tx_status'):
                            self.__status_msg_queue.put(frame)
                        
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
        return True
    # }}}
    
    # {{{ now
    def now(self):
        return datetime.datetime.now()
    # }}}
    


class DatabaseConsumer(BaseConsumer):
    # {{{ __init__
    def __init__(self, db_name, xbee_addresses = [], socket_dest = ('tonidoplug', 9999)):
        import sqlite3
        
        self.dbc = sqlite3.connect(db_name,
                                   isolation_level = None,
                                   timeout = 5)
        
        BaseConsumer.__init__(self, xbee_addresses, socket_dest)
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        BaseConsumer.process_forever(self)
        self.dbc.close()
    # }}}
    


if __name__ == '__main__':
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + '/logs/consumer.log',
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


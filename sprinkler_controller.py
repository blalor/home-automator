#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import daemonizer

import time
import logging, logging.handlers

import signal
import threading

import consumer
import SimpleXMLRPCServer
import random

class SprinklerConsumer(consumer.BaseConsumer):
    # mapping of sprinkler ID to DIO config command
    sprinkler_map = {
        1 : 'D0',
        2 : 'D1',
    }
    
    # {{{ __init__
    def __init__(self, xbee_address):
        consumer.BaseConsumer.__init__(self, xbee_addresses = [xbee_address])
        
        ## only supporting a single address; __init__ parses addresses
        self.xbee_address = self.xbee_addresses[0]
        
        self.__sprinkler_active = {
            1 : False,
            2 : False,
        }
        
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'name':'remote_at_response',
        #  'structure':
        #     [{'name':'frame_id',        'len':1},
        #      {'name':'source_addr_long','len':8},
        #      {'name':'source_addr',     'len':2},
        #      {'name':'command',         'len':2},
        #      {'name':'status',          'len':1},
        #      {'name':'parameter',       'len':None}]},
        
        if frame['id'] == 'remote_at_response':
            self._logger.debug("not handling remote_at_response frame")
            return False
        
        # if frame['id'] != 'remote_at_response':
        #     self._logger.debug("unhandled frame id %s", frame['id'])
        #     return False
        
        self._logger.info("not handling other frame: " + unicode(str(frame), errors='replace'))
        return False
    
    # }}}
    
    # {{{ activate_sprinkler
    def activate_sprinkler(self, sprinkler_id):
        if sprinkler_id not in self.sprinkler_map:
            raise InvalidSprinkler("invalid sprinkler id %s" % (str(sprinkler_id),))
        
        self._logger.info("activating sprinkler %s", sprinkler_id)
        
        success = False
        
        dio_cmd = self.sprinkler_map[sprinkler_id]
        
        if self._send_remote_at(self.xbee_address, command = dio_cmd, param_val = '\x05'):
            success = self._send_remote_at(self.xbee_address, command = 'AC')
        
        if success:
            self.__sprinkler_active[sprinkler_id] = True;
        
        return success
    
    # }}}
    
    # {{{ deactivate_sprinkler
    def deactivate_sprinkler(self, sprinkler_id):
        if sprinkler_id not in self.sprinkler_map:
            raise InvalidSprinkler("invalid sprinkler id %s" % (str(sprinkler_id),))
        
        self._logger.info("deactivating sprinkler %s", sprinkler_id)
        
        success = False
        
        dio_cmd = self.sprinkler_map[sprinkler_id]
        
        if self._send_remote_at(self.xbee_address, command = dio_cmd, param_val = '\x04'):
            success = self._send_remote_at(self.xbee_address, command = 'AC')
        
        if success:
            self.__sprinkler_active[sprinkler_id] = False;
        
        return success
    
    # }}}
    
    # {{{ sprinkler_active
    def sprinkler_active(self, sprinkler_id):
        return self.__sprinkler_active[sprinkler_id]
    
    # }}}
    



def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/sprinkler.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    sc = SprinklerConsumer(xbee_address = '00:11:22:33:44:55:66:1d')
    xrs = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 10103))
    
    try:
        # fire up XMLRPCServer
        xrs.register_introspection_functions()
        xrs.register_function(sc.activate_sprinkler, 'activate_sprinkler')
        xrs.register_function(sc.deactivate_sprinkler, 'deactivate_sprinkler')
        xrs.register_function(sc.sprinkler_active, 'sprinkler_active')
        
        xrs_thread = threading.Thread(target = xrs.serve_forever)
        xrs_thread.daemon = True
        xrs_thread.start()
        
        sc.process_forever()
    finally:
        sc.shutdown()
        xrs.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import daemonizer

import time
import logging, logging.handlers
import re

import signal
import threading

import consumer
import SimpleXMLRPCServer
import random

class FurnaceConsumer(consumer.DatabaseConsumer):
    data_re = re.compile(r'''^([ZT])=(\d+)$''')
    
    # {{{ __init__
    def __init__(self, db_name, xbee_address = []):
        consumer.DatabaseConsumer.__init__(self, db_name, xbee_addresses = [xbee_address])
        
        ## only supporting a single address; __init__ parses addresses
        self.xbee_address = self.xbee_addresses[0]
        
        self.buf = ''
        self.found_start = False
        self.sample_record = {}
        
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': 'T: 770.42 Cm: 375.14 RH:  42.88 Vcc: 3332 tempC:  19.04 tempF:  66.27\r\n',
        #  'source_addr': '\xda\xe0',
        #  'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xdc'}
        
        if frame['id'] != 'zb_rx':
            self._logger.debug("unhandled frame id %s", frame['id'])
            return False
        
        now = self.now()
        
        ## convert the data to chars and append to buffer
        self.buf += frame['rf_data']
        
        ## process each line found in the buffer
        while self.buf.count('\n') > 0:
            eol_ind = self.buf.index('\n')
            line = self.buf[:eol_ind + 1].strip()
            
            ## strip off the line we've just found
            self.buf = self.buf[eol_ind + 1:]
            
            if line == "<begin>":
                if self.found_start:
                    self._logger.error("found <begin> without <end>; discarding collected data")
                
                self.found_start = True
                
                # default to unknown values
                self.sample_record['Z'] = None
                self.sample_record['T'] = None
            
            elif line == "<end>":
                # finalize
                if not self.found_start:
                    self._logger.error("found <end> without <begin>; discarding collected data")
                    
                else:
                    for k in self.sample_record.keys():
                        if self.sample_record[k] == None:
                            self._logger.warn("missing value for %s", k)
                            
                    try:
                        self.dbc.execute(
                            """
                            insert into furnace (ts_utc, zone_active)
                            values (?, ?)
                            """,
                            (
                                time.mktime(now.timetuple()),
                                self.sample_record['Z']
                            )
                        )
                    except:
                        self._logger.error("unable to insert record into database", exc_info = True)
                    
                self.found_start = False
            else:
                match = self.data_re.match(line)
                if not match:
                    self._logger.error("cannot match %s", line)
                    continue
                    
                sample_type, sample_value = match.groups()
                sample_value = int(sample_value)
                
                self.sample_record[sample_type] = sample_value
        
        return True
    
    # }}}
    
    # {{{ start_timer
    def start_timer(self):
        self._logger.info("starting timer")
        return self._send_data(self.xbee_address, 'S')
    
    # }}}
    
    # {{{ cancel_timer
    def cancel_timer(self):
        self._logger.info("cancelling timer")
        return self._send_data(self.xbee_address, 'C')
    
    # }}}
    
    # {{{ get_time_remaining
    def get_time_remaining(self):
        return self.sample_record['T']
    
    # }}}



def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/furnace.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    fc = FurnaceConsumer(basedir + '/sensors.db', xbee_address = '00:11:22:33:44:55:66:4d')
    xrs = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 10101))
    
    try:
        # fire up XMLRPCServer
        xrs.register_introspection_functions()
        xrs.register_function(fc.start_timer, 'start_timer')
        xrs.register_function(fc.cancel_timer, 'cancel_timer')
        xrs.register_function(fc.get_time_remaining, 'get_time_remaining')
        
        xrs_thread = threading.Thread(target = xrs.serve_forever)
        xrs_thread.daemon = True
        xrs_thread.start()
        
        fc.process_forever()
    finally:
        fc.shutdown()
        xrs.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

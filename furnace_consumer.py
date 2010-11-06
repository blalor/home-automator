#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import daemonizer

import time
import logging, logging.handlers
import re

import signal
import threading
import Queue

import consumer
import SimpleXMLRPCServer

class FurnaceConsumer(consumer.DatabaseConsumer):
    data_re = re.compile(r'''^([ZT])=(\d+)$''')
    
    # {{{ __init__
    def __init__(self, db_name, xbee_address = []):
        consumer.DatabaseConsumer.__init__(self, db_name, xbee_addresses = [xbee_address])
        
        ## only supporting a single address
        self.xbee_address = self.xbee_addresses[0]
        
        self.buf = ''
        self.found_start = False
        self.sample_record = {}
        self.frame_id = '\x01'
        
        # queue for packets that aren't explicitly handled in handle_packet,
        # so that __send_data can get to them.
        self.extra_messages_queue = Queue.Queue()
        
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': 'T: 770.42 Cm: 375.14 RH:  42.88 Vcc: 3332 tempC:  19.04 tempF:  66.27\r\n',
        #  'source_addr': '\xda\xe0',
        #  'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xdc'}
        
        # print frame
        
        if frame['id'] != 'zb_rx':
            self._logger.error("unhandled frame id %s", frame['id'])
            self.extra_messages_queue.put(frame)
            return
        
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
                
    
    # }}}
    
    # {{{ __send_data
    def __send_data(self, data):
        success = False
        
        # increment frame_id but constrain to 1..255.  Seems to go
        # 2,4,6,…254,1,3,5…255. Whatever.
        self.frame_id = chr(((ord(self.frame_id) + 1) % 255) + 1)
        
        self.xbee.zb_tx_request(frame_id = self.frame_id, dest_addr_long = self.xbee_address, data = data)
        
        while True:
            try:
                frame = self.extra_messages_queue.get(True, 1)
                
                # print frame
                if (frame['id'] == 'zb_tx_status') and \
                   (frame['frame_id'] == self.frame_id):
                    
                    if frame['delivery_status'] == '\x00':
                        # success!
                        success = True
                        self._logger.debug("sent data with %d retries", ord(frame['retries']))
                    else:
                        self._logger.error("send failed after %d retries with status 0x%2X", ord(frame['retries']), ord(frame['status']))
                    
                    break
            except Queue.Empty:
                pass
            
        return success
    
    # }}}
    
    # {{{ start_timer
    def start_timer(self):
        self._logger.info("starting timer")
        return self.__send_data('S')
    
    # }}}
    
    # {{{ cancel_timer
    def cancel_timer(self):
        self._logger.info("cancelling timer")
        return self.__send_data('C')
    
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

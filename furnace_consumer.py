#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import daemonizer

import time
import logging, logging.handlers
import struct

import signal
import threading

import consumer
import SimpleXMLRPCServer
import random

class FurnaceConsumer(consumer.DatabaseConsumer):
    zone_states = {
        0 : "unknown",
        1 : "active",
        2 : "inactive",
    }
    
    # {{{ __init__
    def __init__(self, db_name, xbee_address = []):
        consumer.DatabaseConsumer.__init__(self, db_name, xbee_addresses = [xbee_address])
        
        ## only supporting a single address; __init__ parses addresses
        self.xbee_address = self.xbee_addresses[0]
        
        self.timer_remaining = -1
    # }}}
    
    # {{{ calc_checksum
    def calc_checksum(self, data):
        chksum = len(data)

        for x in data:
            chksum += ord(x)

        chksum = (0x100 - (chksum & 0xFF))

        return chksum
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        if frame['id'] != 'zb_rx':
            self._logger.debug("unhandled frame id %s", frame['id'])
            return False
        
        now = self.now()
        
        data = frame['rf_data']
        
        if data.startswith('\xff\x55'):
            data_len = ord(data[2])
            
            if self.calc_checksum(data[3:-1]) == ord(data[-1]):
                sample = struct.unpack("<BBBH?HB", data)
                
                zone_state = self.zone_states[sample[3]]
                powered = sample[4]
                self.timer_remaining = sample[5]
                
                self._logger.debug(
                    "zone %s, powered: %s, time remaining: %d" % (
                        zone_state,
                        str(powered),
                        self.timer_remaining
                    )
                )
                
                if zone_state != 'unknown':
                    if zone_state == 'active':
                        db_zone_val = 1
                    else:
                        db_zone_val = 0
                    
                    try:
                        self.dbc.execute(
                            """
                            insert into furnace (ts_utc, zone_active)
                            values (?, ?)
                            """,
                            (
                                time.mktime(now.timetuple()),
                                db_zone_val
                            )
                        )
                    except:
                        self._logger.error("unable to insert record into database", exc_info = True)
                
            else:
                self._logger.warn("bad checksum")
        
        return True
    
    # }}}
    
    # {{{ start_timer
    def start_timer(self, duration = 420):
        self._logger.info("starting timer; duration %d", duration)
        self.timer_remaining = duration
        
        payload = struct.pack("<cH", 'S', duration)
        msg = '\xff\x55%s%s%s' % (
            struct.pack('<B', len(payload)),
            payload,
            struct.pack('<B', self.calc_checksum(payload))
        )
        
        return self._send_data(self.xbee_address, msg)
    
    # }}}
    
    # {{{ cancel_timer
    def cancel_timer(self):
        self._logger.info("cancelling timer")
        self.timer_remaining = 0
        
        payload = struct.pack("<c", 'C')
        msg = '\xff\x55%s%s%s' % (
            struct.pack('<B', len(payload)),
            payload,
            struct.pack('<B', self.calc_checksum(payload))
        )
        
        return self._send_data(self.xbee_address, msg)
    
    # }}}
    
    # {{{ get_time_remaining
    def get_time_remaining(self):
        return self.timer_remaining
    
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

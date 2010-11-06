#!/usr/bin/env python
# encoding: utf-8
"""
xbee_lt_sensor.py

Created by Brian Lalor on 2010-10-29.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys, os
import daemonizer
import time
import consumer
import signal
import struct
import logging, logging.handlers

class InvalidDeviceTypeException(Exception):
    pass


# {{{{ LightTempConsumer
class LightTempConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        now = self.now()
        
        if frame['id'] != 'zb_rx_io_data':
            self._logger.error("unhandled frame id %s", frame['id'])
            return
        
        formatted_addr = self._format_addr(frame['source_addr_long'])
        
        if 'samples' not in frame:
            self._logger.error("no samples in frame!")
            continue
        
        samples = frame['samples'][0]
        
        light = None
        temp_C = None
        temp_F = None
        
        if 'adc-1' not in samples:
            self._logger.warn("missing adc-1 sample")
        else:
            light = samples['adc-1']
        
        if 'adc-2' not in samples:
            self._logger.warn("missing adc-2 sample")
        else:
            temp_C = (self._sample_to_mv(samples['adc-2']) - 500.0) / 10.0
            
            if formatted_addr.lower() == "00:11:22:33:44:55:66:7d":
                # router; adjust temp down 4°C
                temp_C -= 4.0
            
            temp_F = (1.8*temp_C)+32
        
        # humidity = ((sample_to_mv(samples['adc-3']) * 108.2 / 33.2) / 5000.0 - 0.16) / 0.0062
        
        # print '%s sensor reading -- light: %d temp %.1fF/%.1fC' % (formatted_addr, light, temp_F, temp_C)
        
        try:
            if temp_C != None:
                self.dbc.execute(
                    "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        formatted_addr,
                        temp_C,
                    )
                )
            
            if light != None:
                self.dbc.execute(
                    "insert into light (ts_utc, node_id, light_val) values (?, ?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        formatted_addr,
                        light,
                    )
                )
        except:
            self._logger.error("unable to insert record into database", exc_info = True)
    
    # }}}

# }}}}

def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/lt_sensor.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    sc = LightTempConsumer(basedir + '/sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:a5', '00:11:22:33:44:55:66:7d'])
    
    try:
        sc.process_forever()
    finally:
        sc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

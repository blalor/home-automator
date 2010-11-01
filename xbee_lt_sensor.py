#!/usr/bin/env python
# encoding: utf-8
"""
xbee_lt_sensor.py

Created by Brian Lalor on 2010-10-29.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import os

import consumer
import signal
import struct

class InvalidDeviceTypeException(Exception):
    pass


# {{{{ LightTempConsumer
class LightTempConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        now = self.utcnow()
        
        if frame['id'] != 'zb_rx_io_data':
            print >>sys.stderr, "unhandled frame id", frame['id']
            return
        
        formatted_addr = self._format_addr(frame['source_addr_long'])
        
        samples = frame['samples'][0]
        
        light = samples['adc-1']
        
        temp_C = (self._sample_to_mv(samples['adc-2']) - 500.0) / 10.0
        temp_F = (1.8*temp_C)+32
        
        # humidity = ((sample_to_mv(samples['adc-3']) * 108.2 / 33.2) / 5000.0 - 0.16) / 0.0062
        
        # print '%s sensor reading -- light: %d temp %.1fF/%.1fC' % (formatted_addr, light, temp_F, temp_C)
        
        try:
            self.dbc.execute(
                "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)",
                (
                    time.mktime(now.utctimetuple()),
                    formatted_addr,
                    temp_C,
                )
            )
            
            self.dbc.execute(
                "insert into light (ts_utc, node_id, light_val) values (?, ?, ?)",
                (
                    time.mktime(now.utctimetuple()),
                    formatted_addr,
                    light,
                )
            )
        except:
            traceback.print_exc()
    
    # }}}

# }}}}

def main():
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    sc = None
    
    try:
        sc = LightTempConsumer('sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:a5', '00:11:22:33:44:55:66:7d'])
        sc.process_forever()
    finally:
        if sc != None:
            sc.shutdown()


if __name__ == '__main__':
    main()

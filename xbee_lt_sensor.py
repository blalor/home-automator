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
class LightTempConsumer(consumer.BaseConsumer):
    nodes_are_sensors = {
        '00:11:22:33:44:55:66:a5': True,
        '00:11:22:33:44:55:66:7d': True,
    }
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        if frame['id'] == 'zb_rx_io_data':
            formatted_addr = self._format_addr(frame['src_addr_long'])
            
            if formatted_addr not in self.nodes_are_sensors:
                self.xbee.remote_at(command='DD', frame_id = self.next_frame_id(), dest_addr_long = frame['src_addr_long'])
            
            elif self.nodes_are_sensors[formatted_addr]:
                samples = frame['samples'][0]
                
                light = samples['adc-1']
                
                temp_C = (self._sample_to_mv(samples['adc-2']) - 500.0) / 10.0
                temp_F = (1.8*temp_C)+32
                
                # humidity = ((sample_to_mv(samples['adc-3']) * 108.2 / 33.2) / 5000.0 - 0.16) / 0.0062
                
                print '%s sensor reading -- light: %d temp %.1fF/%.1fC' % (formatted_addr, light, temp_F, temp_C)
        
        elif (frame['id'] == 'remote_at_response') and (frame['command'] == 'DD'):
            formatted_addr = self._format_addr(frame['source_addr_long'])
            
            if struct.unpack('>I', frame['parameter'])[0] in (0x03000E, 0x030008):
                self.nodes_are_sensors[formatted_addr] = True
            else:
                self.nodes_are_sensors[formatted_addr] = False
    
    # }}}

# }}}}

def main():
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    sc = None
    
    try:
        sc = LightTempConsumer()
        sc.process_forever()
    finally:
        if sc != None:
            sc.shutdown()


if __name__ == '__main__':
    main()

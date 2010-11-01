#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import consumer
import time
import traceback
import signal

class PowerConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#23:71#\r\n',
        #  'src_addr': '\x18:',
        #  'src_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        now = self.utcnow()
        
        if frame['id'] != 'zb_rx':
            print >>sys.stderr, "unhandled frame id", frame['id']
            return
        
        # #853:0#
        # readings given in amps * 100
        
        data = frame['rf_data'].strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            # print clamp1, clamp2
            
            try:
                self.dbc.execute(
                    "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)",
                    (
                        time.mktime(now.utctimetuple()),
                        clamp1,
                        clamp2,
                    )
                )
            except:
                traceback.print_exc()
        else:
            print >>sys.stderr, "bad data:", data
    # }}}

def main():
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    pc = None
    
    try:
        pc = PowerConsumer('sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:0a'])
        pc.process_forever()
    finally:
        if pc != None:
            pc.shutdown()


if __name__ == '__main__':
    main()

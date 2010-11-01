#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import consumer
import time
import traceback
import signal

class EnvironmentalNodeConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': 'T: 770.42 Cm: 375.14 RH:  42.88 Vcc: 3332 tempC:  19.04 tempF:  66.27\r\n',
        #  'source_addr': '\xda\xe0',
        #  'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xdc'}
        
        now = self.utcnow()
        
        if frame['id'] != 'zb_rx':
            print >>sys.stderr, "unhandled frame id", frame['id']
            return
        
        sdata = frame['rf_data'].strip().split()
        
        # Parse the sample
        # T: 778.21 Cm: 377.93 RH:  47.17 Vcc: 3342 tempC:  13.31 tempF:  55.96
        if sdata[0] == 'T:':
            rel_humid = float(sdata[5])
            temp_C = float(sdata[9])
            
            try:
                self.dbc.execute(
                    "insert into humidity (ts_utc, node_id, rel_humid) values (?, ?, ?)",
                    (
                        time.mktime(now.utctimetuple()),
                        self._format_addr(frame['source_addr_long']),
                        rel_humid,
                    )
                )
                
                self.dbc.execute(
                    "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)",
                    (
                        time.mktime(now.utctimetuple()),
                        self._format_addr(frame['source_addr_long']),
                        temp_C,
                    )
                )
            except:
                traceback.print_exc()
        else:
            print >>sys.stderr, "bad data:", sdata
    # }}}

def main():
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    c = None
    
    try:
        c = EnvironmentalNodeConsumer('sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:dc'])
        c.process_forever()
    finally:
        if c != None:
            c.shutdown()


if __name__ == '__main__':
    main()

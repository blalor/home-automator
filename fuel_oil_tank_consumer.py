#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import consumer
import time
import traceback
import signal

class FuelOilTankConsumer(consumer.DatabaseConsumer):
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
        
        # remove trailing whitespace
        data = frame['rf_data'].strip()
        
        if data:
            height = struct.unpack('<f', data)[0]
            
            try:
                self.dbc.execute(
                    "insert into oil_tank (ts_utc, height) values (?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        height,
                    )
                )
            except:
                traceback.print_exc()
            
        else:
            print >>sys.stderr, "bad data: " + data
    
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

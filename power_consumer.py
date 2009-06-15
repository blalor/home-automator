#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import consumer
import time
import traceback

class PowerConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, xb):
        now = self.utcnow()
        
        # #853:0#
        # readings given in amps * 100
        
        data = ''.join([chr(c) for c in xb.data]).strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            print clamp1, clamp2
            
            try:
                self.dbc.execute(
                    "insert into power (timestamp, clamp1, clamp2) values (?, ?, ?)",
                    (
                        time.mktime(now.timetuple()),
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
    pc = None
    
    try:
        pc = PowerConsumer('sensors.db', xbee_address = '0013A2004053A365')
        pc.process_forever()
    finally:
        if pc != None:
            pc.shutdown()


if __name__ == '__main__':
    main()

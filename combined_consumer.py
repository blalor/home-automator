#!/usr/bin/env python
# encoding: utf-8
"""
combined_consumer.py

Created by Brian Lalor on 2009-06-16.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

import sys
import consumer
import time
import traceback
import signal
import re

class CombinedConsumer(consumer.DatabaseConsumer):
    data_re = re.compile(r'''^([A-Z][A-Za-z]*)\[(\w+)\]=(-?\d+)$''')
    
    # {{{ __init__
    def __init__(self, db_name):
        consumer.DatabaseConsumer.__init__(self, db_name, xbee_addresses = None)
        
        self.buf = ''
        self.found_start = False
        self.sample_record = {}
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, xb):
        if xb.address_64 == '0013A2004053A44D':
            return self.handle_furnace_packet(xb)
        elif xb.address_64 == '0013A2004053A365':
            return self.handle_power_packet(xb)
        
    # }}}
    
    
    # {{{ handle_power_packet
    def handle_power_packet(self, xb):
        now = self.utcnow()
        
        # #853:0#
        # readings given in amps * 100
        
        data = ''.join([chr(c) for c in xb.data]).strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            # print clamp1, clamp2
            
            try:
                self.dbc.execute(
                    "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)",
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
    
    
    # {{{ handle_furnace_packet
    def handle_furnace_packet(self, xb):
        now = self.utcnow()
        
        ## convert the data to chars and append to buffer
        self.buf = self.buf + ''.join([chr(c) for c in xb.data])
        
        ## process each line found in the buffer
        while self.buf.count('\n') > 0:
            eol_ind = self.buf.index('\n')
            line = self.buf[:eol_ind + 1].strip()
            
            ## strip off the line we've just found
            self.buf = self.buf[eol_ind + 1:]
            
            if line == "<begin>":
                if self.found_start:
                    print >>sys.stderr, "found <begin> without <end>; discarding collected data"
                
                self.found_start = True
                
                # default to unknown values
                self.sample_record = {
                    'Tc:sophies_room' : None,
                    'Tc:living_room'  : None,
                    'Tc:outside'      : None,
                    'Tc:basement'     : None,
                    'Tc:master'       : None,
                    'Tc:office'       : None,
                    'Z:master'        : None,
                    'Z:living_room'   : None,
                }
            
            elif line == "<end>":
                # finalize
                if not self.found_start:
                    print >>sys.stderr, "found <end> without <begin>; discarding collected data"
                    
                else:
                    for k in self.sample_record.keys():
                        if self.sample_record[k] == None:
                            print >>sys.stderr, "missing value for", k, "at", now.isoformat()
                            
                    try:
                        self.dbc.execute(
                            """
                            insert into room_temp (
                                ts_utc, sophies_room, living_room, outside,
                                basement, master, office,
                                master_zone, living_room_zone
                            ) values (
                                ?, ?, ?, ?,
                                ?, ?, ?,
                                ?, ?)
                            """,
                            (
                                time.mktime(now.timetuple()),
                                self.sample_record['Tc:sophies_room'],
                                self.sample_record['Tc:living_room'],
                                self.sample_record['Tc:outside'],
                                self.sample_record['Tc:basement'],
                                self.sample_record['Tc:master'],
                                self.sample_record['Tc:office'],
                                self.sample_record['Z:living_room'],
                                self.sample_record['Z:master'],
                            )
                        )
                    except:
                        traceback.print_exc()
                    
                self.found_start = False
            else:
                match = self.data_re.match(line)
                if not match:
                    print >>sys.stderr, "cannot match", line
                    continue
                    
                sample_type, sample_label, sample_value = match.groups()
                sample_value = int(sample_value)
                
                if sample_type == 'Tc':
                    # convert from int/long value sent from Arduino to °C
                    sample_value /= 10000.0
                    
                    # oh, what the hell. put °F in there, too
                    self.sample_record['Tf:' + sample_label] = (sample_value * 1.8) + 32.0
                    
                    
                self.sample_record[sample_type + ':' + sample_label] = sample_value
    # }}}
    


def main():
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    cc = None
    
    try:
        cc = CombinedConsumer('sensors.db')
        cc.process_forever()
    finally:
        if cc != None:
            pc.shutdown()



if __name__ == '__main__':
    main()


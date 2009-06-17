#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import consumer
import time

import traceback

import re

class FurnaceConsumer(consumer.DatabaseConsumer):
    data_re = re.compile(r'''^([A-Z][A-Za-z]*)\[(\w+)\]=(-?\d+)$''')
    
    # {{{ __init__
    def __init__(self, address = None):
        consumer.BaseConsumer.__init__(self, address)
        
        self.buf = ''
        self.found_start = False
        self.sample_record = {}
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, xb):
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
                            print >>sys.stderr, "missing value for", k, "at", self.sample_record['ts']
                            
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
    fc = None
    
    try:
        fc = FurnaceConsumer('sensors.db', xbee_address = '0013A2004053A44D')
        fc.process_forever()
    finally:
        if fc != None:
            fc.shutdown()


if __name__ == '__main__':
    main()

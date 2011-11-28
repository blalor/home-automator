#!/usr/bin/env python
# encoding: utf-8
"""
message_cleaner.py

Created by Brian Lalor on 2011-11-27.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

# # ../
# sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
# 
# from support import time_util

import json
import cPickle as pickle

from pprint import pprint

import random
import datetime, iso8601, pytz, time

SYSTEM_TZ = pytz.timezone(time.tzname[0])

zb_addr_replacements = {}

# {{{ replace_addr
def replace_addr(addr):
    if addr in zb_addr_replacements:
        return zb_addr_replacements[addr]
    else:
        while True:
            replacement = "00:11:22:33:44:55:66:%02x" % (random.randint(0,255))
            
            if replacement not in zb_addr_replacements:
                zb_addr_replacements[addr] = replacement
                
                return replacement

# }}}

# {{{ main
def main():
    epoch = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, pytz.UTC)
    first_timestamp = None
    
    with open(sys.argv[1], "rb", 0) as ifp:
        unpickler = pickle.Unpickler(ifp)
        
        while True:
            try:
                msg = unpickler.load()
                
                if msg['properties'].content_type == 'application/x-python-pickle':
                    msg['body'] = pickle.loads(msg['body'])
                elif msg['properties'].content_type == 'application/json':
                    msg['body'] = json.loads(msg['body'])
                
                if first_timestamp == None:
                    first_timestamp = msg['timestamp']
                
                print '>>> %(exchange)s %(routing_key)s' % msg['method']
                
                # pprint(msg)
                
                msg['timestamp'] = epoch + (msg['timestamp'] - first_timestamp)
                
                if msg['method']['exchange'] == 'sensor_data':
                    # sensor_data electric_meter
                    # sensor_data furnace
                    # sensor_data humidity.<addr>
                    # sensor_data light.<addr>
                    # sensor_data temperature.<addr>
                    
                    topic = msg['method']['routing_key'].split(".", 1)
                    addr = None
                    
                    if len(topic) == 2:
                        topic, addr = topic
                        addr = replace_addr(addr)
                        
                        msg['method']['routing_key'] = '%s.%s' % (topic, addr)
                    
                    if 'node_id' in msg['body']:
                        msg['body']['node_id'] = replace_addr(msg['body']['node_id'])
                    
                    if 'timestamp' in msg['body']:
                        ts = msg['body']['timestamp']
                        
                        ts = epoch + (iso8601.parse_date(ts).astimezone(SYSTEM_TZ) - first_timestamp)
                        
                        msg['body']['timestamp'] = ts.astimezone(pytz.UTC).isoformat()
                        
                elif msg['method']['exchange'] == 'raw_xbee_frames':
                    # raw_xbee_frames zb_rx.<addr>
                    # raw_xbee_frames zb_rx_io_data.<addr>
                    
                    frame_type, addr = msg['method']['routing_key'].split(".", 1)
                    
                    addr = replace_addr(addr)
                    
                    msg['method']['routing_key'] = '%s.%s' % (frame_type, addr)
                    
                    msg['body']['_timestamp'] = epoch + (SYSTEM_TZ.localize(msg['body']['_timestamp']) - first_timestamp)
                    msg['body']['source_addr_long'] = "".join([chr(int(x, 16)) for x in addr.split(":")])
                    
                
                pprint(msg)
                
                if msg['properties'].content_type == 'application/x-python-pickle':
                    msg['body'] = pickle.dumps(msg['body'])
                elif msg['properties'].content_type == 'application/json':
                    msg['body'] = json.dumps(msg['body'])
                
                
            except EOFError:
                break

# }}}

if __name__ == '__main__':
    main()


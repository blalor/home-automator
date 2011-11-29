#!/usr/bin/env python2.6
# encoding: utf-8
"""
xbee_device_config.py

Created by Brian Lalor on 2010-10-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import consumer
import threading
import logging

from pprint import pprint

import struct

from datetime import datetime
import pytz

UTC = pytz.UTC

def format_addr(bytes):
    return ":".join(['%02x' % ord(y) for y in bytes])


class NodeDetails(object):
    def __init__(self, node_id):
        super(NodeDetails, self).__init__()
        self.node_id = node_id
        
        self.frame_count = 0
        self.last_frame = None
    


class Gateway(consumer.BaseConsumer):
    def __init__(self):
        super(Gateway, self).__init__('ALL')
        self.async_replies = {}
        
        ## the nodes we've discovered
        self.__node_details = {}
        
        self._register_rpc_function('zb_net_monitor', self.discover_nodes)
        self._register_rpc_function('zb_net_monitor', self.get_discovered_nodes)
        self._register_rpc_function('zb_net_monitor', self.get_node_details)
    
    
    # {{{ discover_nodes
    def discover_nodes(self):
        req = gw._send_remote_at("00:00:00:00:00:00:00:00", "ND", async = True)
        
    # }}}
    
    # {{{ get_discovered_nodes
    def get_discovered_nodes(self)::
        return self.__node_details.keys()
    
    # }}}
    
    # {{{ get_node_details
    def get_node_details(self, node_id):
        return self.__node_details[node_id]
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, packet):
        node = None
        
        if formatted_addr not in self.__node_details:
            node = self.__node_details[formatted_addr]
        else:
            node = Node(formatted_addr)
        
        node.frame_count += 1
        node.last_frame = UTC.localize(datetime.utcnow())
    
    # }}}
    
    # {{{ handle_async_reply
    def handle_async_reply(self, req, frame):
        if frame['command'] not in self.async_replies:
            self.async_replies[frame['command']] = []
        
        if frame['command'] == 'ND':
            # Node Discover. Discovers and reports all RF modules found. The following
            # information is reported for each module discovered.
            # MY<CR> -- 16-bit network address
            # SH<CR> -- serial number high
            # SL<CR> -- serial number low
            # NI<CR> (Variable length) -- node identifier
            # PARENT_NETWORK ADDRESS (2 Bytes)<CR> 
            # DEVICE_TYPE<CR> (1 Byte: 0=Coord, 1=Router, 2=End Device)
            # STATUS<CR> (1 Byte: Reserved)
            # PROFILE_ID<CR> (2 Bytes)
            # MANUFACTURER_ID<CR> (2 Bytes)
            # <CR>
            #
            # After (NT * 100) milliseconds, the command ends by returning a <CR>. ND
            # also accepts a Node Identifier (NI) as a parameter (optional). In this
            # case, only a module that matches the supplied identifier will respond.
            #
            # If ND is sent through the API, each response is returned as a separate
            # AT_CMD_Response packet. The data consists of the above listed bytes
            # without the carriage return delimiters. The NI string will end in a
            # "0x00" null character. The radius of the ND command is set by the BH
            # command.

            # ~a\x00\x13\xa2\x00@2\xdc\xdcLiving Room T/H\x00\xff\xfe\x01\x00\xc1\x05\x10\x1e\x00\x03\x00\x00
            # ~a\x00\x13\xa2\x00@2\xdc\xdcLiving Room T/H\x00\xff\xfe\x01\x00\xc1\x05\x10\x1e\x00\x03\x00\x00

            # parent network address: \xff\xfe
            # device type: \x01
            # status: \x00
            # profile_id: \xc1\x05
            # manufacturer_id: \x10\x1e
            # <extra?>: \x00\x03\x00\x00

            data = frame['parameter']
            ni_end = data.find('\x00', 10)
            ni_data = {
                'MY' : format_addr(data[:2]),
                'addr' : format_addr(data[2:10]),
                'NI' : data[10:ni_end],
                'parent network address' : format_addr(data[ni_end+1:ni_end+3]),
                'device_type' : data[ni_end+3:ni_end+4],
                'status (reserved)' : data[ni_end+4:ni_end+5],
                'profile_id' : data[ni_end+5:ni_end+7],
                'manufacturer_id' : data[ni_end+7:ni_end+9],
                '<extra?>' : data[ni_end+9:],
            }

            if ni_data['MY'] == 'ff:fe':
                ni_data['MY'] = None

            if ni_data['parent network address'] == 'ff:fe':
                ni_data['parent network address'] = None

            # pprint((data, ni_data))
            self.async_replies[frame['command']].append(ni_data)
        else:
            self.async_replies[frame['command']].append(frame)
    
    # }}}


def main(argv=None):
    gw = Gateway()
    
    gw_thread = threading.Thread(target = gw.process_forever, name = "gw_proc")
    gw_thread.daemon = True
    gw_thread.start()
    
    logging.debug("waiting for consumer to be ready")
    gw.ready_event.wait()
    logging.info("ready")
    
    pprint(gw._send_remote_at("00:00:00:00:00:00:00:00", "NI"))
    nt_secs = struct.unpack(
        '>H',
        gw._send_remote_at("00:00:00:00:00:00:00:00", "NT")['parameter']
    )[0] / 10.0
    
    req = gw._send_remote_at("00:00:00:00:00:00:00:00", "ND", async = True, timeout = nt_secs)
    
    logging.info("got request ticket %s", req.ticket)
    req.event.wait()
    
    # pprint(gw.async_replies)
    gw.async_replies['ND'].append({'addr':'00:00:00:00:00:00:00:00', 'NI':'Coordinator'})
    for y in gw.async_replies['ND']:
        # pprint(y)
        
        pprint((
            y['addr'],
            y['NI'],
            "".join(['%02x' % ord(v) for v in gw._send_remote_at(y['addr'], "VR")['parameter']]),
            struct.unpack('>B', gw._send_remote_at(y['addr'], "NC")['parameter'])[0],
            struct.unpack('>B', gw._send_remote_at(y['addr'], "DB")['parameter'])[0],
            struct.unpack('>H', gw._send_remote_at(y['addr'], "%V")['parameter'])[0],
        ))
        
    
    gw.async_replies.clear()
    
    logging.debug("cleaning up")
    gw.shutdown()
    gw_thread.join()
    logging.shutdown()
    


if __name__ == "__main__":
    from support import log_config
    
    log_config.init_logging_stdout(level = logging.INFO)
    
    sys.exit(main())

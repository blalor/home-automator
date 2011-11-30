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

import time
from datetime import datetime
from pytz import UTC

class NodeDetails(object):
    def __init__(self, node_id):
        super(NodeDetails, self).__init__()
        self.node_id = node_id.lower()
        
        # number of frames seen (since startup)
        self.frame_count = 0
        
        # UTC timestamp (datetime) of last frame seen
        self.last_frame = None
        
        # have the node details been fully populated?
        self.fully_populated = False
        
        # stale means it was not seen during an ND poll
        self.stale = True
        
        self.ident = None           # NI
        self.short_addr = None      # MY
        self.parent_network = None  # MP
        self.node_type = None       # device type from ND
        self.profile_id = None      # profile id from ND
        self.manufacturer_id = None # manufacturer id from ND
        
        self.version = None         # VR
        self.device_type = None     # DD
        self.vcc = None             # %V
        self.rss = None             # DB
    
    
    def saw_node(self):
        """updates activity indicators for this node"""
        self.frame_count += 1
        self.last_frame = UTC.localize(datetime.utcnow())
        self.stale = False
    
    
    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.__dict__)
    


class XBeeNetworkMonitor(consumer.BaseConsumer):
    DEVICE_TYPE_MAP = {
        0 : 'coordinator',
        1 : 'router',
        2 : 'end_device',
    }
    
    COORD_ADDR = "00:00:00:00:00:00:00:00"
    
    def __init__(self):
        super(XBeeNetworkMonitor, self).__init__('ALL')
        
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        ## the nodes we've discovered; addr forced to lower case
        self.__node_details = {}
        self.__node_details_lock = threading.RLock()
        
        self._register_rpc_function('zb_net_monitor', self.discover_nodes)
        self._register_rpc_function('zb_net_monitor', self.get_discovered_nodes)
        self._register_rpc_function('zb_net_monitor', self.get_node_details)
        
        ## pre-populate node details object for coordinator
        ## all sorts of special handling here…
        node = self.__get_node(self.COORD_ADDR)
        node.ident = "<coordinator>"
        node.node_type = self.DEVICE_TYPE_MAP[0]
        node.stale = False
    
    
    # {{{ discover_nodes
    def discover_nodes(self):
        """initiates the node discovery process (XBee ND command)"""
        
        self._logger.info("initiating discovery")
        
        # flag all nodes as stale; they'll get updated by ND replies
        with self.__node_details_lock:
            for node in self.__node_details.values():
                if node.node_id != self.COORD_ADDR:
                    node.stale = True
        
        
        req = self._send_remote_at(
            self.COORD_ADDR, "ND",
            async = True,
            timeout = self.__node_disc_timeout
        )
        
        # wait for it to complete
        req.event.wait()
    
    # }}}
    
    # {{{ get_discovered_nodes
    def get_discovered_nodes(self):
        """returns a list of all the node IDs that have been discovered"""
        return self.__node_details.keys()
    
    # }}}
    
    # {{{ get_node_details
    def get_node_details(self, node_id):
        """returns the NodeDetails instance for the given node ID"""
        
        # we're the only one who knows about the NodeDetails class…
        return self.__node_details[node_id.lower()].__dict__
    
    # }}}
    
    # {{{ __get_node
    def __get_node(self, addr):
        addr = addr.lower()
        
        with self.__node_details_lock:
            if addr in self.__node_details:
                node = self.__node_details[addr]
            else:
                self._logger.info("found new node %s", addr)
                node = NodeDetails(addr)
                self.__node_details[addr] = node
        
        return node
    
    # }}}
    
    # {{{ format_addr
    @staticmethod
    def format_addr(bytes):
        return ":".join(['%02x' % ord(y) for y in bytes])
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, packet):
        """Handles all XBee frames transmitted by the gateway"""
        
        addr = formatted_addr.lower()
        
        node = self.__get_node(addr)
        
        node.saw_node()
    
    # }}}
    
    # {{{ handle_async_reply
    def handle_async_reply(self, req, frame):
        if frame['command'] != 'ND':
            self._logger.error("unhandled request %r: %r", req, frame)
        else:
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
            
            ## WARNING!! XMLRPC marshaller will barf on binary data!
            
            data = frame['parameter']
            nd_end = data.find('\x00', 10) # end of NI string
            
            nd_data = {
                'MY' : self.format_addr(data[:2]),
                'addr' : self.format_addr(data[2:10]),
                'NI' : data[10:nd_end],
                'MP' : self.format_addr(data[nd_end+1:nd_end+3]),
                'node_type' : self.DEVICE_TYPE_MAP[ord(data[nd_end+3:nd_end+4])],
                'status (reserved)' : data[nd_end+4:nd_end+5],
                'profile_id' : "".join(['%02x' % ord(x) for x in data[nd_end+5:nd_end+7]]),
                'manufacturer_id' : "".join(['%02x' % ord(x) for x in data[nd_end+7:nd_end+9]]),
                '<extra?>' : data[nd_end+9:],
            }

            if nd_data['MY'] == 'ff:fe':
                nd_data['MY'] = None

            if nd_data['MP'] == 'ff:fe':
                nd_data['MP'] = None
            
            node = self.__get_node(nd_data['addr'])
            node.ident = nd_data['NI']
            node.short_addr = nd_data['MY']
            node.parent_network = nd_data['MP']
            node.node_type = nd_data['node_type']
            node.profile_id = nd_data['profile_id']
            node.manufacturer_id = nd_data['manufacturer_id']
            
            # this counts as a frame from the node
            node.saw_node()
            
            self._logger.debug("discovered node %r", node)
    
    # }}}
    
    # {{{ __query_param
    def __query_param(self, addr, param):
        param_transforms = {
            'NT' : lambda x: struct.unpack('>H', x)[0] / 10,
            'VR' : lambda x: "".join(['%02x' % ord(v) for v in x]),
            'DD' : lambda x: "".join(['%02x' % ord(v) for v in x]),
            'DB' : lambda x: struct.unpack('>B', x)[0] * -1,
            '%V' : lambda x: (struct.unpack('>H', x)[0] * (1200.0 / 1024.0)) / 1000.0,
        }
        
        assert param in param_transforms, "no transform for param " + param
        
        f = self._send_remote_at(addr, param)
        
        if self._check_remote_at_frame_status(f):
            try:
                return param_transforms[param](f['parameter'])
            except:
                self._logger.error("unable to retrieve value from %r", f, exc_info = True)
        else:
            # status logged in _check_remote_at_frame_status
            # self._logger.error("bad response for param %s from %s", param, addr)
            pass
    
    # }}}
    
    # {{{ __worker
    def __worker(self):
        # wait for the main processing thread to be ready
        self.ready_event.wait()
        
        ## for some reason, the coordinator doesn't respond to its own address
        ## (not COORD_ADDR).  When querying VR, for example…
        # # ensure local device responds to ND
        # if not self._check_remote_at_frame_status(
        #     self._send_remote_at(self.COORD_ADDR, "NO", '\x00')
        # ):
        #     self._logger.error("unable to set NO = 0x00")
        
        # retrieve the node discovery timeout value
        self.__node_disc_timeout = self.__query_param(self.COORD_ADDR, "NT")
        
        self._logger.debug("NT: %d", self.__node_disc_timeout)
        
        # fire off full discovery immediately
        next_full_disco = time.time() - 1
        
        while True:
            try:
                do_full_discovery = (time.time() > next_full_disco)
                
                if do_full_discovery:
                    self._logger.debug("performing full discovery")
                    
                    self.discover_nodes()
                
                # walk over all the known nodes and update them
                with self.__node_details_lock:
                    node_ids = self.__node_details.keys()
                
                for node_id in node_ids:
                    node = self.__node_details[node_id]
                    
                    if do_full_discovery or (not node.fully_populated):
                        self._logger.debug("refreshing properties of %s", node_id)
                        
                        # version
                        node.version = self.__query_param(node_id, "VR")
                        
                        # received signal strength (-dBm)
                        node.rss = self.__query_param(node_id, "DB")
                        
                        # voltage
                        node.vcc = self.__query_param(node_id, "%V")
                        
                        # device type
                        node.device_type = self.__query_param(node_id, "DD")
                        
                        node.fully_populated = True
                        
                        self._logger.debug("node: %r", node)
                        
                        if node.stale:
                            self._logger.info("node %s is stale", node_id)
                        
                
                if do_full_discovery:
                    # full discovery every 15 minutes
                    next_full_disco = time.time() + 900
            
            except consumer.NoResponse:
                # occasionally a request gets dropped on the floor, and I 
                # don't know why!
                self._logger.error("this shouldn't happen!", exc_info = True)
            
            time.sleep(10)
                
        
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        # start a worker thread to update the node info
        worker_thread = threading.Thread(target = self.__worker, name = "worker")
        worker_thread.daemon = True
        worker_thread.start()
        
        # delegate to our parent
        super(XBeeNetworkMonitor, self).process_forever()
    
    # }}}
    


def main(argv=None):
    from support import daemonizer, log_config
    import logging
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/xbee_network_monitor.log")
    
    # log_config.init_logging_stdout()
    
    mon = XBeeNetworkMonitor()
    
    try:
        mon.process_forever()
    
    except:
        logging.fatal("something bad happened", exc_info = True)
        
    finally:
        mon.shutdown()
        log_config.shutdown()
    


if __name__ == "__main__":
    main()

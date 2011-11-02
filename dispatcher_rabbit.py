#!/usr/bin/env python2.6
# encoding: utf-8
"""
dispatcher.py

Created by Brian Lalor on 2009-06-15.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

## http://effbot.org/zone/thread-synchronization.htm
from __future__ import with_statement

import sys, os

import pika
import datetime
import threading
import Queue

import xbee, serial
import struct
import cPickle as pickle

# {{{ _format_addr
def format_addr(addr):
    return ":".join(['%02x' % ord(x) for x in addr])

# }}}

# {{{ serialize
def serialize(data):
    return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

# }}}

# {{{ deserialize
def deserialize(data):
    return pickle.loads(data)

# }}}

class XBeeDispatcher(object):
    RAW_XBEE_PACKET_EXCHANGE = 'raw_xbee_packets'
    
    def __init__(self, rabbit_host, ser):
        super(XBeeDispatcher, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.serial = ser
        
        # set up connection/channel for streaming received XBee packets
        self._xb_rx_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host = rabbit_host)
        )
        
        self._xb_rx_conn_lock = threading.RLock()
        
        self._xb_rx_chan = self._xb_rx_conn.channel()
        self._xb_rx_chan.exchange_declare(
            exchange = self.RAW_XBEE_PACKET_EXCHANGE,
            type = 'topic'
        )
        
        # set up connection/channel for receiving frames to be sent to 
        # XBee devices
        self._xb_tx_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host = rabbit_host)
        )
        
        self._xb_tx_conn_lock = threading.RLock()
        
        self._xb_tx_chan = self._xb_tx_conn.channel()
        self._xb_tx_chan.queue_declare(queue = 'xbee_tx')
        self._xb_tx_chan.basic_qos(prefetch_count = 1)
        self._xb_tx_chan.basic_consume(self.__handle_xb_tx, queue = 'xbee_tx')
        
        tx_consume_thread = threading.Thread(target = self._xb_tx_chan.start_consuming)
        tx_consume_thread.daemon = True
        tx_consume_thread.start()
    
    
    # {{{ __handle_xb_tx
    def __handle_xb_tx(self, chan, method, props, body):
        pass
    
    # }}}
    
    # {{{ __dispatch_xb_frame
    def __dispatch_xb_frame(self, frame):
        """handles incoming XBee frames"""
        
        frame['_timestamp'] = datetime.datetime.now()
        
        correlation_data = None
        frame_addr = 'unknown'
        
        if 'frame_id' in frame:
            # this is a response of some kind
            # self._logger.debug("found reply of type %s with frame_id %02x", frame['id'], ord(frame['frame_id']))
            
            with self.__correlation_lock:
                if frame['frame_id'] in self.__correlations:
                    # hand off the frame to the waiting handler
                    self.__correlations[frame['frame_id']]['response'] = frame
                    self.__correlations[frame['frame_id']]['event'].set()
                else:
                    self._logger.error("no correlation found for response: %s", str(frame))
                
        else:
            if 'source_addr' in frame:
                frame_addr = format_addr(frame['source_addr'])
                
                if 'source_addr_long' in frame:
                    frame_addr = format_addr(frame['source_addr_long'])
            
            # something like "zb_rx.00:11:22:33:44:55:66:0a"
            routing_key = '%s.%s' % (frame['id'], frame_addr)
            
            self._logger.debug("routing_key: %s", routing_key)
            
            with self._xb_rx_conn_lock:
                self._xb_rx_chan.basic_publish(
                    exchange = self.RAW_XBEE_PACKET_EXCHANGE,
                    routing_key = routing_key,
                    body = serialize(frame)
                )
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        ## start XBee communication thread here
        self.xbee_thread = xbee.XBee(self.serial, callback = self.__dispatch_xb_packet)
        
        try:
            pass
        finally:
            # perform shutdown operations
            
            ## close down XBee comm thread
            self.xbee_thread.halt()
            self.serial.close()
            del self.xbee_thread
            
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        pass
    
    # }}}
    
    
    def send_packet(self, name, **kwargs):
        with self.__serial_write_lock:
            self.xbee_thread.send(name, **kwargs)
        
    


class XBeeRequestHandler(SocketServer.BaseRequestHandler):
    def __init__(self, request, client_address, server):
        self._logger = logging.getLogger(self.__class__.__name__)
        
        SocketServer.BaseRequestHandler.__init__(self, request, client_address, server)
    
    
    
    def handle(self):
        ## we *must* ensure that finish() gets called, which only happens when
        ## handle() returns successfully.
        
        header_len = struct.calcsize('!I')
        
        try:
            connection_alive = True
            
            while connection_alive:
                # enable short timeout for receiving data
                self.request.settimeout(0.25)
                
                try:
                    header_dat = self.request.recv(header_len, socket.MSG_WAITALL)
                    if len(header_dat) == 0:
                        self._logger.info('peer closed the connection')
                        connection_alive = False
                        continue
                    elif len(header_dat) != header_len:
                        self._logger.error('%d bytes for header, need %d: %s', len(header_dat), header_len, " ".join(['%.2x' % (ord(c),) for c in header_dat]))
                    else:
                        data_len = struct.unpack('!I', header_dat)[0]
                        packet = pickle.loads(self.request.recv(data_len, socket.MSG_WAITALL))
                        self.server.send_packet(packet[0], **packet[1])
                
                except socket.timeout:
                    # timeout reading data
                    pass
                
                # disable the timeout for sending data
                self.request.settimeout(None)
                
                try:
                    # pull a packet from the queue, with a blocking timeout
                    packet = self.packet_queue.get(True, 1)
                    data = pickle.dumps(packet, pickle.HIGHEST_PROTOCOL)
                    
                    self.request.sendall(struct.pack('!I', len(data)))
                    self.request.sendall(data)
                except Queue.Empty:
                    pass
                except socket.error:
                    self._logger.error("socket error sending packet to client", exc_info = True)
                    
                    connection_alive = False
                    
                    
        except:
            self._logger.error("exception handling request", exc_info = True)
        
    


def main():
    import daemonizer
    import signal
    
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/dispatcher.log")
    
    # log_config.init_logging_stdout()
    
    dispatcher = XBeeDispatcher(
        'pepe',
        serial.Serial(port=sys.argv[1], baudrate=int(sys.argv[2]),rtscts=True)
    )
    
    # The signals SIGKILL and SIGSTOP cannot be caught, blocked, or ignored.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, lambda signum, frame: dispatcher.shutdown())
    signal.signal(signal.SIGTERM, lambda signum, frame: dispatcher.shutdown())
    
    try:
        dispatcher.process_forever()
    except KeyboardInterrupt:
        logging.info("Interrupt caught, shutting down")
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        logging.debug("cleaning up")
        server.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()


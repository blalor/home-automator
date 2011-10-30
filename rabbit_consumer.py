#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# base class(es) for implementing packet consumers from the raw_xbee_packets exchange.

import sys,os
import socket

import pika

import logging
import logging.handlers
import daemonizer
import Queue
import random

import cPickle as pickle

class Disconnected(Exception):
    pass


class InvalidDestination(Exception):
    pass


class BaseConsumer(object):
    # {{{ __init__
    def __init__(self, bindings = ()):
        """
        bindings is a tuple of <frame_type>.address strings
        """
        super(BaseConsumer, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.__frame_id = chr(((random.randint(1, 255)) % 255) + 1)
        self.__shutdown = False
        
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='pepe')
        )
        
        self._channel = self._connection.channel()
        
        self.declare_exchanges(self._channel)
        
        # create new queue exclusively for us
        self._queue_name = self._channel.queue_declare(exclusive = True).method.queue
        
        # configure 
        self._channel.basic_consume(self.__on_receive_packet,
                                    queue = self._queue_name,
                                    no_ack = True)
        
        for binding in bindings:
            self._channel.queue_bind(exchange = 'raw_xbee_packets',
                                     queue = self._queue_name,
                                     routing_key = binding)
        
        # queue for status frames that aren't explicitly handled in handle_packet,
        # so that __send_data can get to them.
        self.__status_msg_queue = Queue.Queue(maxsize = 100)
        
        # queue for remote_at_response frames that aren't explicitly handled in
        # handle_packet, so that _send_remote_at can get to them.
        self.__remote_at_msg_queue = Queue.Queue(maxsize = 100)
    
    # }}}
    
    # {{{ declare_exchanges
    def declare_exchanges(self, channel):
        """
        declares required exchanges; sub-classes should extend.
        
        @todo move to config file (exchanges.ini) ?
        """
        self._logger.debug("declaring exchanges")
        
        channel.exchange_declare(exchange = 'raw_xbee_packets', type = 'topic')
        channel.exchange_declare(exchange = 'sensor_data', type = 'topic')
    
    # }}}
    
    # {{{ _format_addr
    def _format_addr(self, addr):
        return ":".join(['%02x' % ord(x) for x in addr])
    
    # }}}
    
    # {{{ _sample_to_mv
    def _sample_to_mv(self, sample):
        """Converts a raw A/D sample to mV (uncalibrated)."""
        return sample * 1200.0 / 1023
    # }}}
    
    # {{{ get_frame_id
    def get_frame_id(self):
        return self.__frame_id
    
    # }}}
    
    # {{{ next_frame_id
    def next_frame_id(self):
        # increment frame_id but constrain to 1..255.  Seems to go
        # 2,4,6,…254,1,3,5…255. Whatever.
        self.__frame_id = chr(((ord(self.__frame_id) + 1) % 255) + 1)
        
        return self.get_frame_id()
    
    # }}}
    
    # {{{ _send_remote_at
    # "remote_at":
    #     [{'name':'id',              'len':1,        'default':'\x17'},
    #      {'name':'frame_id',        'len':1,        'default':'\x00'},
    #      # dest_addr_long is 8 bytes (64 bits), so use an unsigned long long
    #      {'name':'dest_addr_long',  'len':8,        'default':struct.pack('>Q', 0)},
    #      {'name':'dest_addr',       'len':2,        'default':'\xFF\xFE'},
    #      {'name':'options',         'len':1,        'default':'\x02'},
    #      {'name':'command',         'len':2,        'default':None},
    #      {'name':'parameter',       'len':None,     'default':None}],
    def _send_remote_at(self, dest, command, param_val = None):
        if dest not in self.xbee_addresses:
            raise InvalidDestination("destination address %s is not configured for this consumer" % self._format_addr(dest))
        
        success = False
        
        frame_id = self.next_frame_id()
        
        self.xbee.remote_at(frame_id = frame_id,
                            dest_addr_long = dest,
                            command = command,
                            parameter = param_val)
        
        while True:
            try:
                frame = self.__remote_at_msg_queue.get(True, 1)
                # frame is guaranteed to have id == remote_at_response
                self._logger.debug("remote_at_response: " + unicode(str(frame), errors='replace'))
                
                if (frame['frame_id'] == frame_id):
                    if frame['status'] == '\x00':
                        # success!
                        success = True
                        self._logger.debug("successfully sent remote AT command")
                    elif frame['status'] == '\x01':
                        # error
                        self._logger.error("unspecified error sending remote AT command")
                    elif frame['status'] == '\x02':
                        # invalid command
                        self._logger.error("invalid command sending remote AT command")
                    elif frame['status'] == '\x03':
                        # invalid parameter
                        self._logger.error("invalid parameter sending remote AT command")
                    elif frame['status'] == '\x04':
                        # remote command transmission failed
                        self._logger.error("remote AT command transmission failed")
                    
                    break
            except Queue.Empty:
                pass
        
        return success
        
    # }}}
    
    
    # {{{ _send_data
    # wait_for_ack is a kludge.  when the general event-driven process and the 
    # _send_data are called in the same thread, the __status_msg_queue is 
    # never populated.  sending requires its own thread, I think.
    def _send_data(self, dest, data, wait_for_ack = True):
        if dest not in self.xbee_addresses:
            raise InvalidDestination("destination address %s is not configured for this consumer" % self._format_addr(dest))
        
        success = False
        
        frame_id = self.next_frame_id()
        
        self.xbee.zb_tx_request(frame_id = frame_id, dest_addr_long = dest, data = data)
        
        ack_received = False
        while wait_for_ack and (not ack_received):
            try:
                frame = self.__status_msg_queue.get(True, 1)
                # frame is guaranteed to have id == zb_tx_status
                
                # print frame
                if (frame['frame_id'] == frame_id):
                    if frame['delivery_status'] == '\x00':
                        # success!
                        success = True
                        self._logger.debug("sent data with %d retries", ord(frame['retries']))
                    else:
                        self._logger.error("send failed after %d retries with status 0x%2X", ord(frame['retries']), ord(frame['delivery_status']))
                    
                    ack_received = True
            except Queue.Empty:
                # self._logger.debug("status message queue is empty")
                pass
            
        return success
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        self._channel.start_consuming()
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.info("shutting down")
        
        self.__shutdown = True
        
        self._channel.stop_consuming()
        self._connection.close()
        
        self._logger.info("shutdown complete")
        
    # }}}
    
    # {{{ __on_receive_packet
    def __on_receive_packet(self, ch, method, properties, body):
        self._logger.debug("received packet from exchange %s with routing key %s", method.exchange, method.routing_key)
        
        xbee_frame = pickle.loads(body)
        
        try:
            self.handle_packet(xbee_frame)
        except:
            self._logger.critical("exception handling packet", exc_info = True)
        
        
        # while not self.__shutdown:
        #     try:
        #         frame = self.xbee.wait_read_frame()
        #         
        #         src_addr = None
        #         if 'source_addr_long' in frame:
        #             src_addr = frame['source_addr_long']
        #         
        #         _do_process = True
        #         if self.xbee_addresses:
        #             if (src_addr != None) and (src_addr not in self.xbee_addresses):
        #                 _do_process = False
        #         
        #         # @todo fix filtering of status frames; looks like they're stored for every consumer
        #         if _do_process:
        #             if not self.handle_packet(frame):
        #                 # self._logger.debug("unhandled frame: " + unicode(str(frame), errors='replace'))
        #                 
        #                 if frame['id'] == 'zb_tx_status':
        #                     # prune queue while full
        #                     while self.__status_msg_queue.full():
        #                         self._logger.debug("status message queue full; removing item")
        #                         self.__status_msg_queue.get()
        #                     
        #                     self.__status_msg_queue.put(frame)
        #                 elif frame['id'] == 'remote_at_response':
        #                     # prune queue while full
        #                     while self.__remote_at_msg_queue.full():
        #                         self._logger.debug("remote AT message queue full; removing item")
        #                         self.__remote_at_msg_queue.get()
        #                         
        #                     self.__remote_at_msg_queue.put(frame)
        #                 
        #         else:
        #             # no match on address; filtered.
        #             pass
        #         
        #     except XBeeProxy.PeerDiedError:
        #         self._logger.critical("connection to server went away", exc_info = True)
        #         self.shutdown()
        #     except KeyboardInterrupt:
        #         self._logger.critical("keyboard interrupt")
        #         self.shutdown()
        #     except:
        #         self._logger.critical("exception handling packet", exc_info = True)
        # 
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, packet):
        ## for testing only; subclasses should override
        self._logger.debug(unicode(str(packet), errors='replace'))
        
        return True
    
    # }}}
    
    # {{{ publish_sensor_data
    def publish_sensor_data(self, routing_key, body):
        self._channel.basic_publish(
            exchange = 'sensor_data',
            routing_key = routing_key,
            body = pickle.dumps(body, pickle.HIGHEST_PROTOCOL)
        )
    
    # }}}
    


if __name__ == '__main__':
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + '/logs/consumer.log',
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        BaseConsumer().process_forever()
    finally:
        BaseConsumer().shutdown()
        logging.shutdown()


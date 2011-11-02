#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# base class(es) for implementing packet consumers from the raw_xbee_packets exchange.

import sys,os

import pika

import logging
import threading
import uuid

import cPickle as pickle

class Disconnected(Exception):
    pass


class InvalidDestination(Exception):
    pass


class BaseConsumer(object):
    # {{{ __init__
    def __init__(self, addrs = ('#')):
        """
        bindings is a tuple of <frame_type>.address strings
        """
        super(BaseConsumer, self).__init__()
        
        self.xbee_addresses = addrs
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.__shutdown = False
        
        self.__connection_rlock = threading.RLock()
        
        self._connection = self._create_connection()
        
        # channel for working with raw packets
        self._channel = self._connection.channel()
        
        self.declare_exchanges(self._channel)
        
        # create new queue exclusively for us (channel is arbitrary)
        self._queue_name = self._channel.queue_declare(exclusive = True).method.queue
        
        
        # configure callback for all packets
        self._channel.basic_consume(self.__on_receive_packet,
                                    queue = self._queue_name,
                                    no_ack = True)
        
        # bind routing keys to queue
        for addr in self.xbee_addresses:
            rk = '*.' + addr.lower()
            
            self._logger.debug("binding routing key %s to raw_xbee_packets", rk)
            
            self._channel.queue_bind(exchange = 'raw_xbee_packets',
                                     queue = self._queue_name,
                                     routing_key = rk)
        
        # queue for status frames that aren't explicitly handled in handle_packet,
        # so that __send_data can get to them.
        self.__status_msgs = {}
        self.__status_msgs_rlock = threading.RLock()
        
        # queue for remote_at_response frames that aren't explicitly handled in
        # handle_packet, so that _send_remote_at can get to them.
        self.__remote_at_msgs = {}
        self.__remote_at_msgs_rlock = threading.RLock()
    
    # }}}
    
    # {{{ _create_connection
    def _create_connection(self):
        return pika.BlockingConnection(
            pika.ConnectionParameters(host='pepe')
        )
    
    # }}}
    
    # {{{ _serialize
    def _serialize(self, data):
        return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
    # }}}
    
    # {{{ _deserialize
    def _deserialize(self, data):
        return pickle.loads(data)
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
        channel.exchange_declare(exchange = 'events', type = 'topic')
    
    # }}}
    
    # {{{ _parse_addr
    def _parse_addr(self, addr):
        paddr = None
        
        if addr != None:
            paddr = "".join(chr(int(x, 16)) for x in addr.split(":"))
        
        return paddr
    
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
            raise InvalidDestination("destination address %s is not configured for this consumer" % dest)
        
        success = False
        
        corr_id = str(uuid.uuid4())
        reply_received_event = threading.Event()
        
        with self.__remote_at_msgs_rlock:
            self.__remote_at_msgs[corr_id] = {
                'event' : reply_received_event,
                'response' : None,
            }
        
        with self.__connection_rlock:
            self._channel.basic_publish(
                exchange = '',
                routing_key = 'xbee_tx',
                properties = pika.BasicProperties(
                    reply_to = self._queue_name,
                    correlation_id = corr_id,
                ),
                body = self._serialize({
                    'method' : 'send_remote_at',
                    'dest' : dest,
                    'command' : command,
                    'param_val' : param_val,
                })
            )
        
        # wait 30s for the reply to our call to be received
        reply_received_event.wait(30)
        
        with self.__remote_at_msgs_rlock:
            frame = self.__remote_at_msgs.pop(corr_id)['response']
            
        if reply_received_event.is_set() and (frame != None):
            # frame is guaranteed to have id == remote_at_response
            
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
            
        else:
            self._logger.warn("no remote AT reply received from %s for %s with correlation %s", dest, command, corr_id)
        
        return success
    
    # }}}
    
    
    # {{{ _send_data
    def _send_data(self, dest, data):
        """dest is a formatted address"""
        
        if dest not in self.xbee_addresses:
            raise InvalidDestination("destination address %s is not configured for this consumer" % dest)
        
        success = False
        
        corr_id = str(uuid.uuid4())
        reply_received_event = threading.Event()
        
        with self.__status_msgs_rlock:
            self.__status_msgs[corr_id] = {
                'event' : reply_received_event,
                'response' : None,
            }
        
        with self.__connection_rlock:
            self._channel.basic_publish(
                exchange = '',
                routing_key = 'xbee_tx',
                properties = pika.BasicProperties(
                    reply_to = self._queue_name,
                    correlation_id = corr_id,
                ),
                body = self._serialize({
                    'method' : 'send_data',
                    'dest' : dest,
                    'data' : data,
                })
            )
        
        # wait 30s for the reply to our call to be received
        reply_received_event.wait(30)
        
        with self.__status_msgs_rlock:
            frame = self.__status_msgs.pop(corr_id)['response']
        
        if reply_received_event.is_set() and (frame != None):
            # frame is guaranteed to have id == zb_tx_status
            
            if frame['delivery_status'] == '\x00':
                # success!
                success = True
                self._logger.debug("sent data with %d retries", ord(frame['retries']))
            else:
                self._logger.error(
                    "send failed after %d retries with status 0x%2X",
                    ord(frame['retries']), ord(frame['delivery_status'])
                )
        else:
            self._logger.warn("no packet ack received from %s for %s with correlation %s", dest, data, corr_id)
        
        return success
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        self.__shutdown_event = threading.Event()
        
        t = threading.Thread(target = self._channel.start_consuming, name = "proc_4evr")
        # t.daemon = True
        t.start()
        
        while True:
            if not t.is_alive():
                self._logger.critical("thread %s died", t.name)
                break
            
            if self.__shutdown_event.is_set():
                self._logger.info("shutdown event set")
                break
            else:
                try:
                    self.__shutdown_event.wait(0.01)
                except KeyboardInterrupt:
                    self.__shutdown_event.set()
                
            
        
        with self.__connection_rlock:
            self._channel.stop_consuming()
            self._connection.close()
        
        self._logger.info("shutdown complete")
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.info("shutting down")
        
        self.__shutdown_event.set()
        
        
    # }}}
    
    # {{{ __on_receive_packet
    def __on_receive_packet(self, ch, method, props, body):
        try:
            self._logger.debug("received packet from exchange '%s' with routing key %s and correlation_id %s",
                               method.exchange, method.routing_key, props.correlation_id)
            
            # we get both standard "raw" frames AND RPC replies in this handler
            
            frame = self._deserialize(body)
            
            # differentiate replies from raw packets
            if (props.correlation_id != None) and (method.routing_key == self._queue_name):
                # this is a reply
                if frame['id'] == 'zb_tx_status':
                    with self.__status_msgs_rlock:
                        if props.correlation_id in self.__status_msgs:
                            self.__status_msgs[props.correlation_id]['response'] = frame
                            self.__status_msgs[props.correlation_id]['event'].set()
                        else:
                            self._logger.error("got zb_tx_status reply for unknown correlation %s: %s",
                                               props.correlation_id, frame)
                
                elif frame['id'] == 'remote_at_response':
                    with self.__remote_at_msgs_rlock:
                        if props.correlation_id in self.__remote_at_msgs:
                            self.__remote_at_msgs[props.correlation_id]['response'] = frame
                            self.__remote_at_msgs[props.correlation_id]['event'].set()
                        else:
                            self._logger.error("got remote_at_response reply for unknown correlation %s: %s",
                                               props.correlation_id, frame)
                
            
            else:
                # standard raw packet; guaranteed  to have a routing_key of the form
                # <frame id>.<address>, where the address is one we're subscribed to
                
                frame_type, formatted_addr = method.routing_key.split('.')
                
                self.handle_packet(formatted_addr, frame)
            
        except:
            self._logger.critical("exception handling packet", exc_info = True)
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, packet):
        ## for testing only; subclasses should override
        self._logger.debug(unicode(str(packet), errors='replace'))
    
    # }}}
    
    # {{{ publish_sensor_data
    def publish_sensor_data(self, routing_key, body):
        with self.__connection_rlock:
            self._channel.basic_publish(
                exchange = 'sensor_data',
                routing_key = routing_key,
                body = self._serialize(body)
            )
    
    # }}}
    
    # {{{ publish_event
    def publish_event(self, routing_key, body):
        with self.__connection_rlock:
            self._channel.basic_publish(
                exchange = 'events',
                routing_key = routing_key,
                body = self._serialize(body)
            )
    
    # }}}
    


if __name__ == '__main__':
    import daemonizer
    
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    # log_config.init_logging(basedir + "/logs/base_consumer.log")
    
    log_config.init_logging_stdout()
    
    bc = BaseConsumer()
    
    try:
        bc.process_forever()
    finally:
        bc.shutdown()
        logging.shutdown()


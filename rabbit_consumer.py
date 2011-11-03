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
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self._xbee_addresses = addrs
        self._connection_params = pika.ConnectionParameters(host='pepe')
        
        # connection/channel for working with raw packets
        self.__xb_frame_conn = self._create_broker_connection()
        self.__xb_frame_chan = self.__xb_frame_conn.channel()
        
        self.declare_exchanges(self.__xb_frame_chan)
        
        # create new queue exclusively for us (channel is arbitrary)
        self.__queue_name = self.__xb_frame_chan.queue_declare(exclusive = True).method.queue
        
        # configure callback for all packets
        self.__xb_frame_chan.basic_consume(self.__on_receive_packet,
                                           queue = self.__queue_name,
                                           no_ack = True)
        
        # bind routing keys to queue
        for addr in self._xbee_addresses:
            self.__xb_frame_chan.queue_bind(exchange = 'raw_xbee_packets',
                                            queue = self.__queue_name,
                                            routing_key = '*.' + addr.lower())
        
        # channel for transmitting packets
        self.__rpc_conn = self._create_broker_connection()
        self.__rpc_channel = self.__xb_frame_conn.channel()
        
        # channel and connection for publishing sensor data and events
        self.__publisher_conn = self._create_broker_connection()
        self.__publisher_chan = self.__xb_frame_conn.channel()
        self.__publisher_chan_lock = threading.RLock()
        
        # queue for response frames that aren't explicitly handled in
        # handle_packet, so that __rpc_send_message can get to them.
        self.__rpc_reply_msgs = {}
        self.__rpc_reply_msgs_lock = threading.RLock()
        
        # default; will be set again in process_forever
        self.__main_thread_name = threading.currentThread().name
    
    # }}}
    
    # {{{ _create_broker_connection
    def _create_broker_connection(self):
        return pika.BlockingConnection(self._connection_params)
    
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
    
    # {{{ __rpc_send_message
    def __rpc_send_message(self, dest, msg_body):
        """dest is a formatted address"""
        
        assert self.__main_thread_name != threading.currentThread().name, \
            "DEADLOCK: spawn a new thread"
        
        if dest not in self._xbee_addresses:
            raise InvalidDestination("destination address %s is not configured for this consumer" % dest)
        
        corr_id = str(uuid.uuid4())
        reply_received_event = threading.Event()
        
        with self.__rpc_reply_msgs_lock:
            self.__rpc_reply_msgs[corr_id] = {
                'event' : reply_received_event,
                'response' : None,
            }
        
        self.__rpc_channel.basic_publish(
            exchange = '',
            routing_key = 'xbee_tx',
            properties = pika.BasicProperties(
                reply_to = self.__queue_name,
                correlation_id = corr_id,
            ),
            body = self._serialize(msg_body)
        )
        
        # wait 30s for the reply to our call to be received
        reply_received_event.wait(30)
        
        if not reply_received_event.is_set():
            self._logger.warn("no reply received from %s with correlation %s", dest, corr_id)
        
        with self.__rpc_reply_msgs_lock:
            frame = self.__rpc_reply_msgs.pop(corr_id)['response']
        
        return frame
    
    # }}}
    
    # {{{ _send_remote_at
    def _send_remote_at(self, dest, command, param_val = None):
        frame = self.__rpc_send_message(
            dest,
            {
                'method' : 'send_remote_at',
                'dest' : dest,
                'command' : command,
                'param_val' : param_val,
            }
        )
        
        success = False
        
        if frame != None:
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
        
        return success
    
    # }}}
    
    # {{{ _send_data
    def _send_data(self, dest, data):
        frame = self.__rpc_send_message(
            dest,
            {
                'method' : 'send_data',
                'dest' : dest,
                'data' : data,
            }
        )
        
        success = False
                
        if frame != None:
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
        
        return success
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        self.__shutdown_event = threading.Event()
        
        t = threading.Thread(target = self.__xb_frame_chan.start_consuming, name = "proc_4evr")
        # t.daemon = True
        
        self.__main_thread_name = t.name
        
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
                    self.__shutdown_event.wait(0.5)
                except KeyboardInterrupt:
                    self.__shutdown_event.set()
                
            
        
        with self.__publisher_chan_lock:
            self._channel.stop_consuming()
            self.__xb_frame_conn.close()
            self.__rpc_conn.close()
            self.__publisher_conn.close()
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.info("shutting down")
        
        self.__shutdown_event.set()
        
    # }}}
    
    # {{{ __on_receive_packet
    def __on_receive_packet(self, ch, method, props, body):
        self._logger.debug("received packet from exchange '%s' with routing key %s and correlation_id %s",
                           method.exchange, method.routing_key, props.correlation_id)
        
        frame = self._deserialize(body)
        formatted_addr = method.routing_key.split('.')[0]
        
        # we get both standard "raw" frames AND RPC replies in this handler
        
        # differentiate replies from raw packets
        if (props.correlation_id != None) and (method.routing_key == self.__queue_name):
            # this is a reply
            with self.__rpc_reply_msgs_lock:
                if props.correlation_id in self.__rpc_reply_msgs:
                    self.__rpc_reply_msgs[props.correlation_id]['response'] = frame
                    self.__rpc_reply_msgs[props.correlation_id]['event'].set()
                else:
                    self._logger.error("got %s reply for unknown correlation %s: %s",
                                       frame['id'], props.correlation_id, frame)
            
        else:
            # standard raw packet; guaranteed  to have a routing_key of the form
            # <frame id>.<address>, where the address is one we're subscribed to
            try:
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
        with self.__publisher_chan_lock:
            self.__publisher_chan.basic_publish(
                exchange = 'sensor_data',
                routing_key = routing_key,
                body = self._serialize(body)
            )
    
    # }}}
    
    # {{{ publish_event
    def publish_event(self, routing_key, body):
        with self.__publisher_chan_lock:
            self.__publisher_chan.basic_publish(
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
        log_config.shutdown()


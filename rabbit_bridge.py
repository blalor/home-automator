#!/usr/bin/env python
# encoding: utf-8
"""
rabbit_bridge.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.

Bridges old dispatcher system with new RabbitMQ system.
"""

import sys, os
import datetime

import pika
import consumer

import logging, logging.handlers
import signal, threading

import daemonizer

import cPickle as pickle
from pprint import pprint

def serialize(data):
    return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)


def deserialize(data):
    return pickle.loads(data)


class RabbitBridge(consumer.BaseConsumer):
    # {{{ __init__
    def __init__(self):
        consumer.BaseConsumer.__init__(self)
        
        ## map of correlation IDs, frame IDs, and destinations
        self.__correlations = {}
        self.__correlation_lock = threading.RLock()
        
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='pepe')
        )
        
        # channel for raw XBee packets
        self._pkt_channel = self._connection.channel()
        
        # create the exchange, if necessary
        self._pkt_channel.exchange_declare(exchange = 'raw_xbee_packets',
                                           type = 'topic')
        
        # channel for receiving transmission packets
        self._rpc_channel = self._connection.channel()
       
        # queue for receiving frames to be sent to XBee devices
        self._rpc_channel.queue_declare(queue = 'xbee_tx')
        
        self._rpc_channel.basic_qos(prefetch_count = 1)
        self._rpc_channel.basic_consume(self.handle_xb_tx, queue = 'xbee_tx')
        
        rpc_consume_thread = threading.Thread(target = self._rpc_channel.start_consuming)
        rpc_consume_thread.daemon = True
        rpc_consume_thread.start()
    
    # }}}
    
    # {{{ on_request
    def handle_xb_tx(self, ch, method, props, body):
        try:
            req = deserialize(body)
        except:
            self._logger.error("unable to load pickle")
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return
        
        self._logger.debug(
            "TX method %s for dest %s with correlation ID %s",
            req['method'], req['dest'], props.correlation_id
        )
        
        
        # maintain relationship of correlation IDs, frame IDs, and destination addresses
        frame_id = self.next_frame_id()
        response_received_event = threading.Event()
        
        with self.__correlation_lock:
            self.__correlations[frame_id] = {
                'response' : None,
                'event' : response_received_event,
            }
        
        if req['method'] == 'send_remote_at':
            # {'method' : 'send_remote_at',
            #  'dest' : <addr>,
            #  'command' : …,
            #  'param_val' : …,
            # }
            self.xbee.remote_at(frame_id = frame_id,
                                dest_addr_long = self._parse_addr(req['dest']),
                                command = req['command'],
                                parameter = req['param_val'])
            
        
        elif req['method'] == 'send_data':
            # {'method' : 'send_data',
            #  'dest' : <addr>,
            #  'data' : …,
            # }
            self.xbee.zb_tx_request(frame_id = frame_id,
                                    dest_addr_long = self._parse_addr(req['dest']),
                                    data = req['data'])
            
        
        # ack that the message's been handled
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        # wait for response for 30s
        response_received_event.wait(30)
        
        if not response_received_event.is_set():
            self._logger.warn("no response received for %s", props.correlation_id)
        
        # ok, we should now have the result
        with self.__correlation_lock:
            resp_frame = self.__correlations.pop(frame_id)['response']
        
        ch.basic_publish(
            exchange = '',
            routing_key = props.reply_to,
            properties = pika.BasicProperties(
                correlation_id = props.correlation_id
            ),
            body = serialize(resp_frame)
        )
        
    
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#23:71#\r\n',
        #  'source_addr': '\x18:',
        #  'source_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        frame['_timestamp'] = datetime.datetime.now()
        
        # pprint(frame)
        
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
                    # self._logger.error("got response to a command I didn't send: %s", str(frame))
                    pass
        else:
            if 'source_addr' in frame:
                frame_addr = self._format_addr(frame['source_addr'])
                
                if 'source_addr_long' in frame:
                    frame_addr = self._format_addr(frame['source_addr_long'])
            
            # something like "zb_rx.00:11:22:33:44:55:66:0a"
            routing_key = '%s.%s' % (frame['id'], frame_addr)
            
            self._logger.debug("routing_key: %s", routing_key)
            
            self._pkt_channel.basic_publish(
                exchange = 'raw_xbee_packets',
                routing_key = routing_key,
                body = serialize(frame)
            )
        
        return True
    
    # }}}
    


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/rabbit_bridge.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    rb = RabbitBridge()
    
    try:
        rb.process_forever()
        
    except:
        logging.fatal("something bad happened", exc_info = True)
        
    finally:
        rb.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()


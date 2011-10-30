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

class RabbitBridge(consumer.BaseConsumer):
    # {{{ __init__
    def __init__(self):
        consumer.BaseConsumer.__init__(self)
        
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='pepe')
        )
        
        self._channel = self._connection.channel()
        
        # create the exchange, if necessary
        self._channel.exchange_declare(exchange = 'raw_xbee_packets',
                                       type = 'topic')
        
        # queue for receiving frames to be sent to XBee devices
        self._channel.queue_declare(queue = 'xbee_tx')
        
        self._channel.basic_qos(prefetch_count = 1)
        self._channel.basic_consume(self.handle_xb_tx, queue = 'xbee_tx')
    
    # }}}
    
    # {{{ on_request
    def handle_xb_tx(self, ch, method, props, body):
        req = pickle.loads(body)
        
        if req['method'] == 'send_remote_at':
            # {
            #     'method' : 'send_remote_at',
            #     'dest' : <addr>,
            #     'command' : …,
            #     'param_val' : …,
            # }
            pass
        elif req['method'] == 'send_data':
            # {
            #     'method' : 'send_data',
            #     'dest' : <addr>,
            #     'data' : …,
            # }
            pass
        
        ch.basic_publish(exchange = '',
                         routing_key = props.reply_to,
                         properties = pika.BasicProperties(correlation_id = props.correlation_id),
                         body = 'something')
        
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    # }}}
    
    # {{{ handle_packet
    # parses a packet from the power meter and feeds it to the volt meter
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#23:71#\r\n',
        #  'source_addr': '\x18:',
        #  'source_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        frame['_timestamp'] = datetime.datetime.now()
        
        pprint(frame)
        
        frame_addr = 'unknown'
        
        if 'source_addr_long' in frame:
            frame_addr = frame['source_addr_long']
        elif 'source_addr' in frame:
            frame_addr = frame['source_addr']
        elif frame['id'] == 'zb_tx_status' and 'dest_addr' in frame:
            frame_addr = frame['dest_addr']
        
        # something like "zb_rx.00:11:22:33:44:55:66:0a"
        routing_key = '%s.%s' % (frame['id'], self._format_addr(frame_addr))
        
        # self._logger.debug("routing_key: %s", routing_key)
        print "routing_key: %s" % (routing_key,)
        
        self._channel.basic_publish(
            exchange = 'raw_xbee_packets',
            routing_key = routing_key,
            body = pickle.dumps(frame, pickle.HIGHEST_PROTOCOL)
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
    logging.getLogger().setLevel(logging.INFO)
    
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


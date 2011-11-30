#!/usr/bin/env python
# encoding: utf-8
"""
sensor_data_listener.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

# ../
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from config import config_data as config

import pika
import json

from pprint import pprint

class Listener(object):
    # {{{ __init__
    def __init__(self):
        super(Listener, self).__init__()
        
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config.message_broker.host)
        )
        
        self._channel = self._connection.channel()
        
        # create new queue exclusively for us
        q_result = self._channel.queue_declare(exclusive = True)
        self._queue_name = q_result.method.queue
        
        self._channel.basic_consume(self.handle_packet,
                                    queue = self._queue_name,
                                    no_ack = True)
        
        # listen for all messages
        self._channel.queue_bind(exchange = 'sensor_data',
                                 queue = self._queue_name,
                                 routing_key = "#")
        
        self._channel.queue_bind(exchange = 'events',
                                 queue = self._queue_name,
                                 routing_key = "#")
        
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, channel, method, properties, body):
        pprint((method, properties, json.loads(body)))
    
    # }}}
    
    # {{{ consume
    def consume(self):
        self._channel.start_consuming()
    
    
    # }}}




def main():
    listener = Listener()
    listener.consume()




if __name__ == '__main__':
    main()


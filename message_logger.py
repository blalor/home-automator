#!/usr/bin/env python
# encoding: utf-8
"""
message_logger.py

Created by Brian Lalor on 2011-10-30.

Pickles all received messages from all configured exchanges into a file.  To 
be used for message analysis and sample data for public examples.
"""

import sys
import os

import pika

from pprint import pprint
from config import config_data as config

import cPickle as pickle
from datetime import datetime

class Listener(object):
    # {{{ __init__
    def __init__(self):
        super(Listener, self).__init__()
        
        self.ofp = open("logs/captured_messages.p", "ab", 0)
        self.pickler = pickle.Pickler(self.ofp, pickle.HIGHEST_PROTOCOL)
        
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
        for exchange in config.message_broker.exchanges:
            self._channel.queue_bind(exchange = exchange,
                                     queue = self._queue_name,
                                     routing_key = "#")
        
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, channel, method, properties, body):
        p = {
            'timestamp': datetime.now(),
            'method': { # Basic.Deliver can't be pickled
                'routing_key' : method.routing_key,
                'exchange' : method.exchange,
            },
            'properties' : properties,
            'body' : body,
        }
        
        # pprint(p)
        self.pickler.dump(p)
        self.ofp.flush()
    
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


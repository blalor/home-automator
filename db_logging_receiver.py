#!/usr/bin/env python
# encoding: utf-8
"""
sensor_data_listener.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import logging
import logging.handlers

import time
import pika
import cPickle as pickle

import sqlite3

class Listener(object):
    ROUTING_KEYS = (
        'electric_meter',
        'light.*',
        'temperature.*',
        'humidity.*',
        'oil_tank',
    )
    
    # {{{ __init__
    def __init__(self):
        super(Listener, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        basedir = os.path.abspath(os.path.dirname(__file__))
        
        self.dbc = sqlite3.connect(basedir + '/sensors.db',
                                   isolation_level = None,
                                   timeout = 5)
        
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='pepe')
        )
        
        self._channel = self._connection.channel()
        
        # create new queue exclusively for us
        q_result = self._channel.queue_declare(exclusive = True)
        self._queue_name = q_result.method.queue
        
        self._channel.basic_consume(self.handle_packet,
                                    queue = self._queue_name,
                                    no_ack = True)
        
        # bind to routing keys
        for rk in self.ROUTING_KEYS:
            self._channel.queue_bind(exchange = 'sensor_data',
                                     queue = self._queue_name,
                                     routing_key = rk)
        
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, channel, method, properties, body):
        frame = pickle.loads(body)
        
        query = None
        data = ()
        
        if method.routing_key == 'electric_meter':
            query = "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['clamp1_amps'],
                frame['clamp2_amps'],
            )
        
        elif method.routing_key.startswith('temperature.'):
            query = "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['node_id'],
                frame['temp_C'],
            )
        
        elif method.routing_key.startswith('light.'):
            query = "insert into light (ts_utc, node_id, light_val) values (?, ?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['node_id'],
                frame['light'],
            )
        
        elif method.routing_key.startswith('humidity.'):
            query = "insert into humidity (ts_utc, node_id, rel_humid) values (?, ?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['node_id'],
                frame['rel_humid'],
            )
        
        elif method.routing_key == 'oil_tank':
            query = "insert into oil_tank (ts_utc, height) values (?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['height'],
            )
        
        else:
            self._logger.critical("UNKNOWN ROUTING KEY %s", method.routing_key)
        
        if query and data:
            self._logger.debug("%s %s", frame['timestamp'].isoformat(), str((query, data)))
            
            try:
                self.dbc.execute(query, data)
            except:
                self._logger.critical("error executing query %s", str((query, data)), exc_info = True)
    
    # }}}
    
    # {{{ consume
    def consume(self):
        self._channel.start_consuming()
    
    
    # }}}




def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    handler = logging.handlers.RotatingFileHandler(basedir + '/logs/db_logger.log',
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        Listener().consume()
    finally:
        logging.shutdown()
    




if __name__ == '__main__':
    main()


#!/usr/bin/env python
# encoding: utf-8
"""
sensor_data_listener.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import logging

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
        'furnace',
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
        q_result = self._channel.queue_declare(queue = 'db_inserts')
        self._queue_name = q_result.method.queue
        
        self._channel.basic_consume(self.__on_receive_message,
                                    queue = self._queue_name)
        
        # bind to routing keys
        for rk in self.ROUTING_KEYS:
            try:
                self._channel.queue_bind(self.__queue_did_bind,
                                         nowait = True,
                                         exchange = 'sensor_data',
                                         queue = self._queue_name,
                                         routing_key = rk)
            except:
                self._logger.error("error binding", exc_info = True)
        
    
    # }}}
    
    # {{{ __queue_did_bind
    def __queue_did_bind(self, *args):
        self._logger.debug("queue did bind: %s", args)
    
    # }}}
    
    # {{{ __on_receive_message
    def __on_receive_message(self, channel, method, properties, body):
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
        
        elif method.routing_key == 'furnace':
            query = "insert into furnace (ts_utc, zone_active) values (?, ?)"
            data = (
                time.mktime(frame['timestamp'].timetuple()),
                frame['zone_active']
            )
        
        else:
            self._logger.critical("UNKNOWN ROUTING KEY %s", method.routing_key)
        
        if query and data:
            self._logger.debug("%s %s", frame['timestamp'].isoformat(), str((query, data)))
            
            can_ack = False
            try:
                self.dbc.execute(query, data)
                can_ack = True
            except sqlite3.IntegrityError:
                self._logger.error("IntegrityError executing query %s", str((query, data)), exc_info = True)
                can_ack = True
            except:
                self._logger.critical("error executing query %s", str((query, data)), exc_info = True)
            
            if can_ack:
                channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        try:
            self._channel.start_consuming()
        finally:
            self._channel.stop_consuming()
            self._connection.close()
    
    # }}}




def main():
    import daemonizer
    
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    # log_config.init_logging(basedir + "/logs/db_logger.log")
    
    log_config.init_logging_stdout()
    
    try:
        Listener().process_forever()
    finally:
        log_config.shutdown()
    


if __name__ == '__main__':
    main()


#!/usr/bin/env python
# encoding: utf-8
"""
db_logging_receiver.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import logging

import time
import pika
import cPickle as pickle

import sqlite3
import copy

class DBLogger(object):
    """docstring for DBLogger"""
    
    ROUTING_KEYS = (
        'electric_meter',
        'light.*',
        'temperature.*',
        'humidity.*',
        'oil_tank',
        'furnace',
    )
    
    # {{{ __init__
    def __init__(self, host):
        super(DBLogger, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        parameters = pika.ConnectionParameters(host = host)
        self.connection = pika.SelectConnection(parameters, self.on_connected)
        
        self.channel = None
        self.dbc = sqlite3.connect(os.path.abspath(os.path.dirname(__file__)) + '/sensors.db',
                                   isolation_level = None,
                                   timeout = 5)
        
        
    
    # }}}
    
    def on_connected(self, conn):
        self._logger.debug("connection opened")
        self.connection.channel(self.on_channel_open)
    
    
    def on_channel_open(self, chan):
        self.channel = chan
        
        self._logger.debug("channel opened: %s", self.channel)
        
        # create new queue 
        self.channel.queue_declare(queue = 'db_inserts', callback = self.on_queue_declared)
    
    
    def do_queue_bind(self, exchange, queue_name, routing_key):
        # see on_queue_declared or why this method is here
        
        self.channel.queue_bind(exchange = exchange,
                                queue = queue_name,
                                routing_key = routing_key,
                                callback = lambda f: self.queue_did_bind(routing_key, queue_name, exchange, f))
    
    
    def on_queue_declared(self, frame):
        self._logger.debug("queue declared: %s", frame)
        
        for rk in self.ROUTING_KEYS:
            # need the do_queue_bind method in order to pass the routing key 
            # to the queue_did_bind callback. callback doesn't get any 
            # information on WHAT was bound (routing key, queue name, 
            # exchange, etc), so to provide it I have to do the lambda 
            # jiggery pokery, but due to scoping problems, the lambda always
            # executes with the "current" value of the routing key, and since
            # the first queue_did_bind callback is called after the last 
            # queue_bind invocation, it always gets self.ROUTING_KEYS[-1] for
            # the routing key
            self.do_queue_bind('sensor_data', frame.method.queue, rk)
        
        self.channel.basic_consume(self.on_receive_message,
                                   queue = frame.method.queue)
        
    
    # {{{ queue_did_bind
    def queue_did_bind(self, routing_key, queue_name, exchange, frame):
        self._logger.debug("queue %s bound to exchange %s with %s", queue_name, exchange, routing_key)
    
    # }}}
    
    # {{{ on_receive_message
    def on_receive_message(self, channel, method, properties, body):
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
                self.channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # }}}
    
    def process_forever(self):
        try:
            self.connection.ioloop.start()
        finally:
            self._logger.info("cleaning up")
            self.connection.close()
            self.connection.ioloop.start()
        


def main():
    import daemonizer
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    # log_config.init_logging(basedir + "/logs/db_logger.log")
    
    log_config.init_logging_stdout()
    
    dbl = DBLogger('pepe')
    
    try:
        dbl.process_forever()
    except KeyboardInterrupt:
        pass
    except:
        logging.getLogger().critical("uncaught exception", exc_info = True)
    finally:
        log_config.shutdown()
    

if __name__ == '__main__':
    main()

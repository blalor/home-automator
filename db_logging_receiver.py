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

ROUTING_KEYS = (
    'electric_meter',
    'light.*',
    'temperature.*',
    'humidity.*',
    'oil_tank',
    'furnace',
)

_logger = logging.getLogger("DBLogger")

connection = None
channel = None
dbc = None

def on_connected(conn):
    global connection
    
    connection = conn
    
    _logger.debug("connection opened")
    connection.channel(on_channel_open)


def on_channel_open(chan):
    global channel
    
    channel = chan
    
    _logger.debug("channel opened: %s", channel)
    
    # create new queue exclusively for us
    channel.queue_declare(queue = 'db_inserts', callback = on_queue_declared)


def on_queue_declared(frame):
    _logger.debug("queue declared: %s", frame)
    
    channel.basic_consume(on_receive_message,
                          queue = frame.method.queue)
    
    for rk in ROUTING_KEYS:
        channel.queue_bind(lambda *a: queue_did_bind(frame.method.queue, rk, *a),
                           exchange = 'sensor_data',
                           queue = frame.method.queue,
                           routing_key = rk)
    


def queue_did_bind(queue_name, rk, *args):
    _logger.debug("queue %s did bind with routing key %s: %s", queue_name, rk, args)


# {{{ on_receive_message
def on_receive_message(channel, method, properties, body):
    global dbc
    
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
        _logger.critical("UNKNOWN ROUTING KEY %s", method.routing_key)
    
    if query and data:
        _logger.debug("%s %s", frame['timestamp'].isoformat(), str((query, data)))
        
        can_ack = False
        try:
            dbc.execute(query, data)
            can_ack = True
        except sqlite3.IntegrityError:
            _logger.error("IntegrityError executing query %s", str((query, data)), exc_info = True)
            can_ack = True
        except:
            _logger.critical("error executing query %s", str((query, data)), exc_info = True)
        
        if can_ack:
            channel.basic_ack(delivery_tag = method.delivery_tag)

# }}}


def main():
    import daemonizer
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    # log_config.init_logging(basedir + "/logs/db_logger.log")
    
    log_config.init_logging_stdout()
    
    global dbc
    dbc = sqlite3.connect(basedir + '/sensors.db',
                          isolation_level = None,
                          timeout = 5)
    
    parameters = pika.ConnectionParameters(host = 'pepe')
    connection = pika.SelectConnection(parameters, on_connected)
    
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        connection.close()
        connection.ioloop.start()
    finally:
        log_config.shutdown()
    

if __name__ == '__main__':
    main()

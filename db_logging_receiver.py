#!/usr/bin/env python
# encoding: utf-8
"""
db_logging_receiver.py

Created by Brian Lalor on 2011-10-30.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import logging

import datetime, time
import pytz
import iso8601

import pika
import json

import sqlite3
import copy

SYSTEM_TZ = pytz.timezone(time.tzname[0])

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
    def __init__(self, host, db_file):
        super(DBLogger, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        parameters = pika.ConnectionParameters(host = host)
        self.connection = pika.SelectConnection(parameters, self.on_connected)
        
        self.channel = None
        
        self.dbc = sqlite3.connect(db_file,
                                   isolation_level = None,
                                   timeout = 5)
        
        self.latest_event_received = {}
    
    
    # }}}
    
    def on_connected(self, conn):
        self._logger.info("connection opened")
        
        self.connection.add_on_close_callback(
            lambda _conn: self._logger.warn("connection closed")
        )
        
        self.connection.channel(self.on_channel_open)
    
    
    def on_channel_open(self, chan):
        self.channel = chan
        
        self._logger.info("channel opened: %s", self.channel)
        
        self.channel.add_on_close_callback(
            lambda code, msg: self._logger.warn("channel closed: %d %s", code, msg)
        )
        
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
        try:
            frame = json.loads(body)
        except ValueError:
            self._logger.error("unable to deserialize message body", exc_info = True)
            self.channel.basic_ack(delivery_tag = method.delivery_tag)
            return
        
        
        rk = method.routing_key
        timestamp = iso8601.parse_date(frame['timestamp']).astimezone(SYSTEM_TZ)
        
        latest_event = SYSTEM_TZ.localize(datetime.datetime.fromtimestamp(0))
        
        if rk in self.latest_event_received:
            latest_event = max(latest_event, self.latest_event_received[rk])
            
            if timestamp < latest_event:
                self._logger.warn(
                    "message out of sequence for %s; %s < %s",
                    rk, timestamp.isoformat(), latest_event.isoformat()
                )
            
        
        self.latest_event_received[rk] = timestamp
        
        query = None
        data = ()
        
        if method.routing_key == 'electric_meter':
            query = "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['clamp1_amps'],
                frame['clamp2_amps'],
            )
        
        elif method.routing_key.startswith('temperature.'):
            query = "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['node_id'],
                frame['temp_C'],
            )
        
        elif method.routing_key.startswith('light.'):
            query = "insert into light (ts_utc, node_id, light_val) values (?, ?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['node_id'],
                frame['light'],
            )
        
        elif method.routing_key.startswith('humidity.'):
            query = "insert into humidity (ts_utc, node_id, rel_humid) values (?, ?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['node_id'],
                frame['rel_humid'],
            )
        
        elif method.routing_key == 'oil_tank':
            query = "insert into oil_tank (ts_utc, height) values (?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['height'],
            )
        
        elif method.routing_key == 'furnace':
            query = "insert into furnace (ts_utc, zone_active) values (?, ?)"
            data = (
                time.mktime(timestamp.timetuple()),
                frame['zone_active']
            )
        
        else:
            self._logger.critical("UNKNOWN ROUTING KEY %s", method.routing_key)
        
        if query and data:
            self._logger.debug("%s %s", timestamp.isoformat(), str((query, data)))
            
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
            
            if self.connection.is_open:
                self.connection.close()
            
            self.connection.ioloop.start()
        
    
    def shutdown(self):
        self._logger.info("shutdown initiated")
        
        if self.connection.is_open:
            self.connection.close()
    
        


def main():
    import daemonizer, signal
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    ## standard config
    daemonizer.createDaemon()
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    log_config.init_logging(basedir + "/logs/db_logger.log")
    
    ## debug config
    # log_config.init_logging_stdout()
    
    try:
        dbl = DBLogger('pepe', basedir + '/sensors.db')
        
        signal.signal(signal.SIGQUIT, lambda signum, frame: dbl.shutdown())
        signal.signal(signal.SIGTERM, lambda signum, frame: dbl.shutdown())
        signal.signal(signal.SIGINT, lambda signum, frame: dbl.shutdown())
        
        dbl.process_forever()
    except:
        logging.critical("uncaught exception", exc_info = True)
    finally:
        log_config.shutdown()
    

if __name__ == '__main__':
    main()

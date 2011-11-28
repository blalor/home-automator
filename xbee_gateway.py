#!/usr/bin/env python2.6
# encoding: utf-8
"""
xbee_gateway.py

Created by Brian Lalor on 2011-11-01.

This is a gateway/router application that shuffles messages between the
message broker and the XBee network. All received XBee frames are published
to the raw_xbee_frames exchange, with the addition of a _timestamp key whose
value is a datetime instance. XBee frame transmission to remote devices are
handled via the RPC mechanism detailed in the RabbitMQ tutorial[1]; messages
are consumed from the xbee_tx queue, and replies are published with the
appropriate reply-to and correlation-id properties. Message bodies are
serialized with pickle; JSON doesn't support binary data.

[1] http://www.rabbitmq.com/tutorials/tutorial-six-python.html
"""

import sys, os
import random
import datetime
import threading
import logging

import pika
import xbee, serial

import cPickle as pickle

# {{{ serialize
def serialize(data):
    return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

# }}}

# {{{ deserialize
def deserialize(data):
    return pickle.loads(data)

# }}}

# {{{ parse_addr
def parse_addr(addr):
    paddr = None
    
    if addr != None:
        paddr = "".join(chr(int(x, 16)) for x in addr.split(":"))
    
    return paddr

# }}}

# {{{ format_addr
def format_addr(addr):
    return ":".join(['%02x' % ord(x) for x in addr])

# }}}

class XBeeDispatcher(object):
    RAW_XBEE_PACKET_EXCHANGE = 'raw_xbee_frames'
    
    # {{{ __init__
    def __init__(self, broker_host, serial_port, baudrate):
        super(XBeeDispatcher, self).__init__()
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.__serial_port = serial_port
        self.__baudrate = baudrate
        
        self.__connection_params = pika.ConnectionParameters(host = broker_host)
        
        self.xbee = None
        
        self.__frame_id = chr(((random.randint(1, 255)) % 255) + 1)
        
        self.__correlations = {}
        self.__correlation_lock = threading.RLock()
    
    # }}}
    
    # {{{ __next_frame_id
    def __next_frame_id(self):
        # increment frame_id but constrain to 1..255.  Seems to go
        # 2,4,6,…254,1,3,5…255. Whatever.
        self.__frame_id = chr(((ord(self.__frame_id) + 1) % 255) + 1)
        
        return self.__frame_id
    
    # }}}
    
    # {{{ __handle_xb_tx
    def __handle_xb_tx(self, chan, method, props, body):
        ## no need for a connection lock since we're running in the main
        ## thread, and there isn't any possibility for concurrent access
        ## to the connection for this thread
        
        try:
            # ack the message; there's nothing below that would be 
            # recoverable if the same message were received again
            chan.basic_ack(delivery_tag = method.delivery_tag)
            
            req = deserialize(body)
            
            self._logger.debug(
                "TX method %s for dest %s with correlation ID %s",
                req['method'], req['dest'], props.correlation_id
            )
            
            # mapping of frame id to message properties; used to direct the
            # response
            frame_id = self.__next_frame_id()
            
            with self.__correlation_lock:
                self.__correlations[frame_id] = {
                    'props' : props,
                }
            
            if req['method'] == 'send_remote_at':
                # {'method' : 'send_remote_at',
                #  'dest' : <addr>,
                #  'command' : …,
                #  'param_val' : …,
                # }
                self.xbee.remote_at(frame_id = frame_id,
                                    dest_addr_long = parse_addr(req['dest']),
                                    command = req['command'],
                                    parameter = req['param_val'])
                
            
            elif req['method'] == 'send_data':
                # {'method' : 'send_data',
                #  'dest' : <addr>,
                #  'data' : …,
                # }
                self.xbee.zb_tx_request(
                    frame_id = frame_id,
                    dest_addr_long = parse_addr(req['dest']),
                    data = req['data']
                )
                
            
            self._logger.debug("XBee command sent and message ack'd")
            
        except:
            self._logger.error("failed processing XBee TX message",
                               exc_info = True)
    
    # }}}
    
    # {{{ __dispatch_xb_frame
    def __dispatch_xb_frame(self, frame):
        """handles incoming XBee frames"""
        
        # @todo make utctime, but this will affect the db logger, and the 
        # plotter, too!
        frame['_timestamp'] = datetime.datetime.now()
        
        try:
            if 'frame_id' in frame:
                # this is a response of some kind
                self._logger.debug(
                    "found reply of type %s with frame_id %02x",
                    frame['id'], ord(frame['frame_id'])
                )
                
                with self.__correlation_lock:
                    if frame['frame_id'] in self.__correlations:
                        props = self.__correlations.pop(frame['frame_id'])['props']
                        
                        self._xb_rx_chan.basic_publish(
                            exchange = '',
                            routing_key = props.reply_to,
                            properties = pika.BasicProperties(
                                correlation_id = props.correlation_id,
                                content_type = 'application/x-python-pickle'
                            ),
                            body = serialize(frame)
                        )
                    else:
                        self._logger.error(
                            "no correlation found for response: %s",
                            str(frame)
                        )
            
            else:
                frame_addr = 'unknown'
                
                if 'source_addr' in frame:
                    frame_addr = format_addr(frame['source_addr'])
                    
                    if 'source_addr_long' in frame:
                        frame_addr = format_addr(frame['source_addr_long'])
                
                # something like "zb_rx.00:11:22:33:44:55:66:0a"
                routing_key = '%s.%s' % (frame['id'], frame_addr)
                
                self._logger.debug("routing_key: %s", routing_key)
                
                self._xb_rx_chan.basic_publish(
                    exchange = self.RAW_XBEE_PACKET_EXCHANGE,
                    routing_key = routing_key,
                    properties = pika.BasicProperties(
                        content_type = 'application/x-python-pickle'
                    ),
                    body = serialize(frame)
                )
        
        except:
            self._logger.error(
                "unhandled exception processing incoming frame",
                exc_info = True
            )
        
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        # create the XBee instance and provide a callback for handling frames
        ser = serial.Serial(port = self.__serial_port,
                            baudrate = self.__baudrate,
                            rtscts=True)
        
        # set up connection/channel for sending received XBee packets to the
        # broker
        self._xb_rx_conn = pika.BlockingConnection(self.__connection_params)
        self._xb_rx_chan = self._xb_rx_conn.channel()
        
        # ok, RX channel is created, so we can safely fire up the XBee thread
        self.xbee = xbee.XBee(ser,
                              shorthand = True,
                              callback = self.__dispatch_xb_frame)
        
        # set up connection/channel for receiving frames to be sent to 
        # XBee devices
        self._xb_tx_conn = pika.BlockingConnection(self.__connection_params)
        
        self._xb_tx_chan = self._xb_tx_conn.channel()
        self._xb_tx_chan.queue_declare(queue = 'xbee_tx')
        self._xb_tx_chan.basic_qos(prefetch_count = 1)
        self._xb_tx_chan.basic_consume(self.__handle_xb_tx, queue = 'xbee_tx')
        
        try:
            self._xb_tx_chan.start_consuming()
        finally:
            self._logger.debug("shutting down after process_forever")
            
            # perform shutdown operations
            self._xb_tx_chan.stop_consuming()
            
            ## close down XBee comm thread
            self.xbee.halt()
            ser.close()
            
            # self._xb_tx_chan.close()
            self._xb_tx_conn.close()
            self._xb_rx_conn.close()
            
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.debug("in shutdown")
        
        if self._xb_tx_conn.is_open:
            self._xb_tx_chan.stop_consuming()
        
        # wait for XBee thread to die
        if self.xbee != None:
            self.xbee.join()
        
        # wait for connections to close
        while self._xb_rx_conn.is_open or self._xb_tx_conn.is_open:
            pass
    
    # }}}


def main():
    from support import daemonizer, log_config
    from config import config_data as config
    
    import signal
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/dispatcher.log")
    
    # log_config.init_logging_stdout()
    
    dispatcher = XBeeDispatcher(
        broker_host = config.message_broker.host,
        serial_port = sys.argv[1],
        baudrate = int(sys.argv[2])
    )
    
    # The signals SIGKILL and SIGSTOP cannot be caught, blocked, or ignored.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, lambda signum, frame: dispatcher.shutdown())
    signal.signal(signal.SIGTERM, lambda signum, frame: dispatcher.shutdown())
    
    try:
        dispatcher.process_forever()
    except KeyboardInterrupt:
        logging.info("Interrupt caught, shutting down")
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        logging.debug("cleaning up")
        dispatcher.shutdown()
        log_config.shutdown()
    


if __name__ == '__main__':
    main()


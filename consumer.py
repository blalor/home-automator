#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# base class(es) for implementing XBee frame consumers from the
# raw_xbee_frames exchange.

# load configuration data
from config import config_data as config

import sys,os

import pika

import logging
import threading
import Queue
import uuid

import cPickle as pickle
import json

import datetime, time, pytz

SYSTEM_TZ = pytz.timezone(time.tzname[0])

class InvalidDestination(Exception):
    pass


class InvalidSerializationContentType(Exception):
    pass


class NoResponse(Exception):
    pass


# {{{ serialize
def serialize(data):
    return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

# }}}

# {{{ deserialize
def deserialize(data):
    return pickle.loads(data)

# }}}

# {{{ serialize_json
def serialize_json(data):
    """serializes an object to JSON, with handling for datetime instances"""
    
    def dthandler(obj):
        if isinstance(obj, datetime.datetime):
            dt = obj
            
            if dt.tzinfo == None:
                # naive
                dt = SYSTEM_TZ.localize(obj)
            
            return dt.astimezone(pytz.utc).isoformat()
        
        elif isinstance(obj, Exception):
            return {
                'py/class': obj.__class__.__name__,
                'args': obj.args,
                'message': obj.message
            }
        else:
            raise TypeError(repr(obj) + " is not JSON serializable")
    
    return json.dumps(data, default=dthandler)

# }}}

# {{{ deserialize_json
# @todo turn timestamp into datetime
def deserialize_json(data):
    return json.loads(data)

# }}}


# {{{{ class XBeeRequest
class XBeeRequest(object):
    """Encapsulation of state data for transmitting outbound XBee frames"""
    
    # {{{ __init__
    def __init__(self, dest, msg_body, async, timeout = None):
        super(XBeeRequest, self).__init__()
        
        self.ticket = str(uuid.uuid4())
        self.event = threading.Event()
        
        self.dest = dest
        self.msg_body = msg_body
        self.async = async
        
        if timeout == None:
            timeout = 120
        
        # request expires in 2 minutes
        self.__expiration = time.time() + timeout
        
        self.response = None
    
    # }}}
    
    # {{{ is_expired
    def is_expired(self):
         return time.time() > self.__expiration
    
    # }}}
    
    # {{{ __repr__
    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.__dict__)
    
    # }}}

# }}}}

# {{{{ class RPCRequest
class RPCRequest(object):
    """Encapsulation of state data for handling RPC invocations"""
    
    # {{{ __init__
    def __init__(
        self,
        correlation_id = None,
        content_type = None,
        reply_to = None,
        callback = None,
        args = None
    ):
        
        super(RPCRequest, self).__init__()
        
        self.correlation_id = correlation_id
        self.content_type   = content_type
        self.reply_to       = reply_to
        self.callback       = callback
        self.args           = args
        
        self.response = None
    
    # }}}
    
    # {{{ __repr__
    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.__dict__)
    
    # }}}

# }}}}

# {{{{ class RPCWorker
class RPCWorker(threading.Thread):
    """Threaded worker class for handling RPC invocations"""
    
    # {{{ __init__
    def __init__(self, conn_params):
        super(RPCWorker, self).__init__(target = self.__runner)
        
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        self.__connection_params = conn_params
        
        self.__pending_requests = Queue.Queue()
        self.__shutdown_event = threading.Event()
    
    # }}}
    
    # {{{ add_request
    def add_request(self, req):
        """Enqueues an RPCRequest"""
        
        self.__pending_requests.put(req)
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        """Initiates an orderly shutdown of this thread"""
        
        self._logger.debug("shutdown requested")
        self.__shutdown_event.set()
    
    # }}}
    
    # {{{ __on_returned_msg
    def __on_returned_msg(self, *args):
        self._logger.error("returned message %r", args)
    
    # }}}
    
    # {{{ __runner
    def __runner(self):
        """
        The thread target; handles RPC invocations and publishes the result
        """
        
        conn = pika.BlockingConnection(self.__connection_params)
        chan = conn.channel()
        
        chan.add_on_return_callback(self.__on_returned_msg)
        
        while not self.__shutdown_event.is_set():
            try:
                req = self.__pending_requests.get(True, 5)
                
                self._logger.debug("invoking %r", req)
                
                response = {}
                
                try:
                    response['result'] = req.callback(*req.args)
                except:
                    self._logger.warn("exception invoking %r", req, exc_info = True)
                    
                    # the exception value
                    response['exception'] = sys.exc_info()[1]
                
                if req.content_type == 'application/x-python-pickle':
                    resp_body = serialize(response)
                elif req.content_type == 'application/json':
                    resp_body = serialize_json(response)
                else:
                    raise InvalidSerializationContentType(req.content_type)
                
                chan.basic_publish(
                    exchange='',
                    routing_key = req.reply_to,
                    properties = pika.BasicProperties(
                        correlation_id = req.correlation_id,
                        content_type = req.content_type,
                    ),
                    body = resp_body
                )
            except Queue.Empty:
                pass
            
        
        conn.close()
    
    # }}}

# }}}}

class BaseConsumer(object):
    """
    Base class for implementing consumers of XBee frames.  Most concrete 
    classes will only need to implement handle_packet().
    """
    
    # {{{ __init__
    # @param addrs tuple of addresses to receive frames for, or "ALL" to receive all
    def __init__(self, addrs = ('#')):
        super(BaseConsumer, self).__init__()
        
        self._logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        self.__allow_all_addrs = False
        
        if addrs == 'ALL':
            self.__allow_all_addrs = True
            addrs = ('#')
        
        self._xbee_addresses = addrs
        
        self.ready_event = threading.Event()
        
        self._connection_params = pika.ConnectionParameters(
            host = config.message_broker.host
        )
        
        self.__queue_name = None
        
        self.__publisher_conn = None
        self.__publisher_chan = None
        
        self.__rpc_conn = None
        self.__rpc_chan = None
        
        self.__xb_frame_conn = None
        self.__xb_frame_chan = None
        self.__rpc_receive_chan = None
        
        self.__rpc_worker = RPCWorker(self._connection_params)
        
        # queue for response frames that aren't explicitly handled in
        # handle_packet, so that __send_xb_frame can get to them.
        self.__pending_xb_requests = {}
        self.__pending_xb_requests_lock = threading.RLock()
        
        # map used for configuring rpc methods. map contains a map of 
        #   queue => [function]
        self.__rpc_queue_map = {}
        
        # default; will be set again in process_forever
        self.__main_thread_name = threading.currentThread().name
    
    # }}}
    
    # {{{ _create_broker_connection
    def _create_broker_connection(self):
        """returns a new broker connection"""
        
        return pika.BlockingConnection(self._connection_params)
    
    # }}}
    
    # {{{ _serialize
    def _serialize(self, data):
        return serialize(data)
    
    # }}}
    
    # {{{ _deserialize
    def _deserialize(self, data):
        return deserialize(data)
    
    # }}}
    
    # {{{ _sample_to_mv
    def _sample_to_mv(self, sample):
        """Converts a raw A/D sample to mV (uncalibrated)."""
        return sample * 1200.0 / 1023
    
    # }}}
    
    # {{{ _register_rpc_function
    def _register_rpc_function(self, queue, func, func_name = None):
        assert self.__xb_frame_chan == None, "call from __init__"
        
        if func_name == None:
            func_name = func.__name__
        
        if queue not in self.__rpc_queue_map:
            self.__rpc_queue_map[queue] = {}
        
        self.__rpc_queue_map[queue][func_name] = func
    
    # }}}
    
    # {{{ check_frame_status
    def _check_remote_at_frame_status(self, frame):
        # frame id should be remote_at_response
        
        command = frame['command']
        success = False
        
        if frame['status'] == '\x00':
            # success!
            success = True
            self._logger.debug("successfully sent remote AT command %s", command)
        
        elif frame['status'] == '\x01':
            # error
            self._logger.error("unspecified error sending remote AT command %s", command)
        
        elif frame['status'] == '\x02':
            # invalid command
            self._logger.warn("invalid command sending remote AT command %s", command)
        
        elif frame['status'] == '\x03':
            # invalid parameter
            self._logger.error("invalid parameter sending remote AT command %s", command)
        
        elif frame['status'] == '\x04':
            # remote command transmission failed
            self._logger.info("remote AT command %s transmission failed", command)
        
        return success
    
    # }}}
    
    # {{{ __send_xb_frame
    # if async is true, let the main message handler deal with the responses.
    # this is to facilitate commands which might return multiple responses, 
    # like the ND (node discover) remote AT command
    def __send_xb_frame(self, dest, msg_body, async = False, timeout = None):
        """dest is a formatted address"""
        
        assert self.__main_thread_name != threading.currentThread().name, \
            "DEADLOCK: spawn a new thread"
        
        if (not self.__allow_all_addrs) and (dest not in self._xbee_addresses):
            raise InvalidDestination("destination address %s is not configured for this consumer" % dest)
        
        req = XBeeRequest(dest, msg_body, async, timeout)
        
        with self.__pending_xb_requests_lock:
            self.__pending_xb_requests[req.ticket] = req
        
        with self.__rpc_chan_lock:
            # keep_alive tells the remote end to keep the request around for 
            # multiple replies
            self.__rpc_chan.basic_publish(
                exchange = '',
                routing_key = 'xbee_tx',
                properties = pika.BasicProperties(
                    reply_to = self.__queue_name,
                    correlation_id = req.ticket,
                    content_type = 'application/x-python-pickle',
                    headers = dict(keep_alive = str(async))
                ),
                immediate = True,
                mandatory = True,
                body = serialize(req.msg_body)
            )
        
        if not async:
            # wait 30s for the reply to our call to be received
            req.event.wait(30)
            
            if not req.event.is_set():
                raise NoResponse("no reply received for %r" % req)
            
            return req.response
        else:
            return req
    
    # }}}
    
    # {{{ _send_remote_at
    def _send_remote_at(self, dest, command, param_val = None, async = False, timeout = None):
        # if async is true, retVal is a request ticket, otherwise it's the 
        # XBeeRequest instance
        return self.__send_xb_frame(
            dest,
            {
                'method' : 'send_remote_at',
                'dest' : dest,
                'command' : command,
                'param_val' : param_val,
            },
            async,
            timeout
        )
    
    # }}}
    
    # {{{ _send_data
    def _send_data(self, dest, data):
        frame = self.__send_xb_frame(
            dest,
            {
                'method' : 'send_data',
                'dest' : dest,
                'data' : data,
            }
        )
        
        success = False
        
        # frame is guaranteed to have id == zb_tx_status
        
        if frame['delivery_status'] == '\x00':
            # success!
            success = True
            self._logger.debug("sent data with %d retries", ord(frame['retries']))
        else:
            self._logger.warn(
                "send failed after %d retries with status 0x%2X",
                ord(frame['retries']), ord(frame['delivery_status'])
            )
        
        return success
    
    # }}}
    
    # {{{ __on_returned_packet
    def __on_returned_packet(self, *args):
        self._logger.error("undeliverable packet %r", args)
    
    # }}}

    # {{{ __on_channel_close
    def __on_channel_close(self, *args):
        self._logger.error("channel closed %r", args)
    
    # }}}
    
    # {{{ __run_thread
    def __run_thread(self):
        try:
            self.__main_thread_name = threading.currentThread().name
            
            # connection/channel for working with raw packets
            self.__xb_frame_conn = self._create_broker_connection()
            self.__xb_frame_chan = self.__xb_frame_conn.channel()
            
            self.__xb_frame_chan.add_on_return_callback(self.__on_returned_packet)
            self.__xb_frame_chan.add_on_close_callback(self.__on_channel_close)
            
            # channel for transmitting XBee frames
            self.__rpc_conn = self._create_broker_connection()
            self.__rpc_chan = self.__rpc_conn.channel()
            self.__rpc_chan_lock = threading.RLock()
            
            
            # channel and connection for publishing sensor data and events
            self.__publisher_conn = self._create_broker_connection()
            self.__publisher_chan = self.__xb_frame_conn.channel()
            self.__publisher_chan_lock = threading.RLock()
            
            # create new queue exclusively for us (channel is arbitrary)
            self.__queue_name = self.__xb_frame_chan.queue_declare(exclusive = True).method.queue
            
            # configure callback for all packets
            self.__xb_frame_chan.basic_consume(self.__on_receive_packet,
                                               queue = self.__queue_name,
                                               no_ack = True)
            
            # bind routing keys to queue
            for addr in self._xbee_addresses:
                self.__xb_frame_chan.queue_bind(exchange = 'raw_xbee_frames',
                                                queue = self.__queue_name,
                                                routing_key = '*.' + addr.lower())
            
            
            if self.__rpc_queue_map:
                self.__rpc_worker.start()
                
                # need a separate channel for RPC requests so that they can be ack'd
                self._logger.debug("creating channel for RPC requests")
                
                self.__rpc_receive_chan = self.__xb_frame_conn.channel()
                
                self.__rpc_receive_chan.basic_qos(prefetch_count=1)
                
                # declare all RPC queues configured via _register_rpc_function.
                # do this on __rpc_receive_chan to direct all incoming messags to 
                # __on_receive_rpc_request
                for queue in self.__rpc_queue_map:
                    self._logger.debug("creating RPC queue %s", queue)
                    
                    self.__rpc_receive_chan.queue_declare(
                        queue = queue,
                        exclusive = True
                    )
                    
                    self.__rpc_receive_chan.basic_consume(
                        self.__on_receive_rpc_request,
                        queue = queue
                    )
            
            
            self.ready_event.set()
            
            self.__xb_frame_chan.start_consuming()
        except:
            self._logger.critical("unhandled exception in __run_thread", exc_info = True)
            # thread will exit
        
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        self._logger.info("starting up")
        
        self.__shutdown_event = threading.Event()
        
        proc_4evr = threading.Thread(target = self.__run_thread, name = "proc_4evr")
        # t.daemon = True
        proc_4evr.start()
        
        def reaper():
            while True:
                self.__reap_unfinished_requests()
                time.sleep(2)
            
        
        reaper_thread = threading.Thread(target = reaper, name = "reaper")
        reaper_thread.daemon = True
        reaper_thread.start()
        
        while True:
            if not proc_4evr.is_alive():
                self._logger.critical("thread %s died", proc_4evr.name)
                break
            
            if self.__shutdown_event.is_set():
                self._logger.info("shutdown event set")
                break
            else:
                try:
                    self.__shutdown_event.wait(0.5)
                except KeyboardInterrupt:
                    self.__shutdown_event.set()
                
            
        
        with self.__publisher_chan_lock:
            # shut down the RPC worker first, so we don't get hung if any of
            # the following fails
            if self.__rpc_worker.is_alive():
                self.__rpc_worker.shutdown()
                self.__rpc_worker.join()
            
            self.__xb_frame_chan.stop_consuming()
            self.__xb_frame_conn.close()
            
            self.__rpc_conn.close()
            self.__publisher_conn.close()
            
            
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.warn("shutting down")
        
        self.__shutdown_event.set()
    
    # }}}
    
    # {{{ __on_receive_rpc_request
    def __on_receive_rpc_request(self, ch, method, props, body):
        self._logger.debug("received RPC req from exchange '%s' with routing key %s and correlation_id %s",
                           method.exchange, method.routing_key, props.correlation_id)
        
        # the following frames are handled here:
        # • RPC request messages
        #      - sent to empty exchange
        #      - routing_key is queue provided in _register_rpc_function
        
        # sanity check
        assert method.routing_key in self.__rpc_queue_map, \
            "%s not a valid RPC queue" % (method.routing_key,)
        
        # hand off the RPC request to the RPCWorker
        if props.content_type == 'application/x-python-pickle':
            req_body = deserialize(body)
        elif props.content_type == 'application/json':
            req_body = deserialize_json(body)
        else:
            raise InvalidSerializationContentType(props.content_type)
        
        callback = None
        if req_body['command'] in self.__rpc_queue_map[method.routing_key]:
            callback = self.__rpc_queue_map[method.routing_key][req_body['command']]
            
            self.__rpc_worker.add_request(
                RPCRequest(
                    correlation_id = props.correlation_id,
                    content_type = props.content_type,
                    reply_to = props.reply_to,
                    callback = callback,
                    args = req_body['args']
                )
            )
        else:
            self._logger.error("command %s not configured for queue %s", req_body['command'], method.routing_key)
        
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    # }}}
    
    # {{{ __on_receive_packet
    def __on_receive_packet(self, ch, method, props, body):
        self._logger.debug("received packet from exchange '%s' with routing key %s and correlation_id %s",
                           method.exchange, method.routing_key, props.correlation_id)
        
        frame = deserialize(body)
        
        # the following frames are handled here:
        #  • XBee "raw" frames
        #      - sent to exhange 'raw_xbee_frames'
        #      - routing_key is '<frame type>.<source addr>'
        #  • XBee reply frames in response to a command or data sent to a device
        #      - sent to empty exchange
        #      - routing_key is name of our private queue
        #      - have correlation_id matching a key in __pending_xb_requests
        
        # differentiate replies from raw packets
        if (props.correlation_id != None) and (method.routing_key == self.__queue_name):
            # this is a reply
            with self.__pending_xb_requests_lock:
                if props.correlation_id in self.__pending_xb_requests:
                    req = self.__pending_xb_requests[props.correlation_id]
                    
                    if not req.async:
                        self.finish_async_request(req, frame)
                    else:
                        self.handle_async_reply(req, frame)
                    
                else:
                    self._logger.error("got %s reply for unknown correlation %s: %s",
                                       frame['id'], props.correlation_id, frame)
            
            # this needs to get called every now and then
            self.__reap_unfinished_requests()
            
        else:
            # standard raw packet; guaranteed  to have a routing_key of the form
            # <frame id>.<address>, where the address is one we're subscribed to
            formatted_addr = method.routing_key.split('.')[1]
            
            try:
                self.handle_packet(formatted_addr, frame)
            except:
                self._logger.critical("exception handling packet", exc_info = True)
            
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, packet):
        ## for testing only; subclasses should override
        self._logger.debug(unicode(str(packet), errors='replace'))
    
    # }}}
    
    # {{{ finish_async_request
    def finish_async_request(self, req, frame = None):
        if frame:
            req.response = frame
        
        req.event.set()
        
        del self.__pending_xb_requests[req.ticket]
    
    # }}}
    
    # {{{ __reap_unfinished_requests
    def __reap_unfinished_requests(self):
        with self.__pending_xb_requests_lock:
            for req in self.__pending_xb_requests.values():
                if req.is_expired():
                    self._logger.debug("expiring %r", req)
                    
                    self.finish_async_request(req)
    
    # }}}
    
    # {{{ handle_async_reply
    def handle_async_reply(self, req, frame):
        ## for testing only; subclasses should override
        self._logger.debug("request %r, frame %r", req, frame)
    
    # }}}
    
    # {{{ publish_sensor_data
    def publish_sensor_data(self, routing_key, body):
        with self.__publisher_chan_lock:
            self.__publisher_chan.basic_publish(
                exchange = 'sensor_data',
                routing_key = routing_key,
                properties = pika.BasicProperties(
                    content_type = 'application/json'
                ),
                body = serialize_json(body)
            )
    
    # }}}
    
    # {{{ publish_event
    def publish_event(self, routing_key, event, **kwargs):
        with self.__publisher_chan_lock:
            self.__publisher_chan.basic_publish(
                exchange = 'events',
                routing_key = routing_key,
                properties = pika.BasicProperties(
                    content_type = 'application/json'
                ),
                body = serialize_json({
                    'name' : event,
                    'data' : kwargs,
                })
            )
    
    # }}}
    


if __name__ == '__main__':
    from support import daemonizer, log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # daemonizer.createDaemon()
    # log_config.init_logging(basedir + "/logs/base_consumer.log")
    
    log_config.init_logging_stdout()
    
    bc = BaseConsumer()
    
    try:
        bc.process_forever()
    finally:
        bc.shutdown()
        log_config.shutdown()

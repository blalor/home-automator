#!/usr/bin/env python
# encoding: utf-8
"""
xmlrpc_server.py

Created by Brian Lalor on 2011-11-04.
Copyright (c) 2011 Pearson Education. All rights reserved.
"""

import sys, os
import traceback
import signal
import time

import uuid

import SimpleXMLRPCServer

import threading, Queue
import logging

import cPickle as pickle

import pika

# shorter select timeout
pika.adapters.select_connection.SelectPoller.TIMEOUT = 0.25

class RejectedMessageException(Exception):
    pass

class RequestTimedOutException(Exception):
    pass


        
class RPCRequest(object):
    """docstring for RPCRequest"""
    def __init__(self, queue, command, args):
        super(RPCRequest, self).__init__()
        self.ticket = str(uuid.uuid4())
        self.event = threading.Event()
        
        self.queue = queue
        self.command = command
        self.args = args
        
        self.exception = None
    
    
    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.__dict__)
    


class BrokerWorker(threading.Thread):
    """docstring for BrokerWorker"""
    
    # {{{ __init__
    def __init__(self, host):
        super(BrokerWorker, self).__init__(
            name = 'BrokerWorker',
            target = self.__run
        )
        
        self.__parameters = pika.ConnectionParameters(host = host)
        
        self._logger = logging.getLogger(self.__class__.__name__)
        
        self.__channel = None
        self.__private_queue_name = None
        
        self.__shutdown = False
        
        self.__pending_requests = {}
        self.__pending_requests_lock = threading.RLock()
        
        self.__rpc_queue = Queue.Queue()
    
    # }}}
    
    # {{{ __run
    def __run(self):
        self._logger.debug("in run")
        
        try:
            self.__connection = pika.SelectConnection(self.__parameters, self.__on_connected)
            
            # prime the timeout pump
            self.__handle_timeout()
            
            print self.__connection.ioloop.poller

            self.__connection.ioloop.start()
        finally:
            self.__shutdown = True
            
            if self.__connection:
                self.__connection.close()
            
            # self.__connection.ioloop.start()
            
        
    # }}}
    
    # {{{ __on_connected
    def __on_connected(self, conn):
        self._logger.info("connection opened")
        
        self.__connection.add_on_close_callback(self.__on_connection_closed)
        
        self.__connection.channel(self.__on_channel_open)
    
    # }}}
    
    # {{{ __on_connection_closed
    def __on_connection_closed(self, conn):
        self._logger.warn("connection closed")
        
        self.__connection = None
    
    # }}}
    
    # {{{ __on_channel_open
    def __on_channel_open(self, chan):
        self.__channel = chan
        
        self._logger.info("channel opened: %s", self.__channel)
        
        self.__channel.add_on_close_callback(
            lambda code, msg: self._logger.warn("channel closed: %d %s", code, msg)
        )
        
        self.__channel.add_on_return_callback(self.__handle_rejected_rpc_request)
        
        # create new queue exclusively for us
        self.__channel.queue_declare(exclusive = True,
                                     callback = self.__on_private_queue_declared)
    
    # }}}
    
    # {{{ __on_private_queue_declared
    def __on_private_queue_declared(self, frame):
        self.__private_queue_name = frame.method.queue
        
        self._logger.debug("queue declared: %s", self.__private_queue_name)
        
        self.__channel.basic_qos(prefetch_count = 1)
        
        # configure callback RPC response packets
        self.__channel.basic_consume(self.__on_receive_rpc_response,
                                     queue = self.__private_queue_name)
        
    
    # }}}
    
    # {{{ __handle_timeout
    def __handle_timeout(self):
        # self._logger.debug("timeout fired")
        
        if not self.__shutdown:
            self.__connection.add_timeout(0.5, self.__handle_timeout)
        
        while not self.__rpc_queue.empty():
            request = self.__pending_requests[self.__rpc_queue.get()]
            
            self._logger.debug("publishing message for ticket %s", request.ticket)
            
            self.__channel.basic_publish(
                exchange = '',
                routing_key = request.queue,
                mandatory = True,
                properties = pika.BasicProperties(
                    reply_to = self.__private_queue_name,
                    correlation_id = request.ticket,
                ),
                body = pickle.dumps({
                    'command' : request.command,
                    'args' : request.args,
                })
            )
    
    # }}}
    
    # {{{ __on_receive_rpc_response
    def __on_receive_rpc_response(self, channel, method, props, body):
        # props.correlation_id is the request ticket
        with self.__pending_requests_lock:
            request = self.__pending_requests.pop(props.correlation_id)
        
        resp_data = pickle.loads(body)
        
        if 'exception' in resp_data:
            request.exception = resp_data['exception']
        else:
            request.response = resp_data['result']
        
        request.event.set()
        
        channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # }}}
    
    # {{{ __handle_rejected_rpc_request
    def __handle_rejected_rpc_request(self, method, header, body):
        request_token = header.properties.correlation_id
        
        self._logger.critical("rejected command: %s", request_token)
        
        request = self.__pending_requests[request_token]
        
        request.exception = RejectedMessageException(method.method.reply_text)
        request.event.set()
    
    # }}}
    
    # {{{ rpc_command
    def rpc_command(self, queue, command, args):
        request = RPCRequest(queue, command, args)
        
        self._logger.debug("processing request %s", request)
        
        with self.__pending_requests_lock:
            self.__pending_requests[request.ticket] = request
            
        self.__rpc_queue.put(request.ticket)
        request.event.wait(30)
        
        if not request.event.is_set():
            raise RequestTimedOutException("not even a rejection?")
        
        if request.exception:
            raise request.exception
        
        return request.response
    
    # }}}
    
    # {{{ shutdown
    def shutdown(self):
        self._logger.info("shutdown initiated")
        self.__shutdown = True
        
        if self.__connection.is_open:
            self.__connection.close()
    
    # }}}
    
class MessagingProxy(object):
    """docstring for MessagingProxy"""
    
    # {{{ __init__
    def __init__(self, worker):
        super(MessagingProxy, self).__init__()
        
        self.__worker = worker
    
    # }}}
    
    # {{{ _dispatch
    def _dispatch(self, method_str, params):
        try:
            queue, method = method_str.split('.')
        except ValueError:
            raise Exception("only queue.method allowed")
        
        return self.__worker.rpc_command(queue, method, params)
    
    # }}}


def stacktraces():
    code = []
    
    for threadId, stack in sys._current_frames().items():
        code.append("\n>>> # ThreadID: %s" % threadId)
        
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
                
        code.append("\n<<<")

    print "\n".join(code)


def run_stacktracer():
    while True:
        stacktraces()
        time.sleep(10)
    



def main():
    import log_config
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/xmlrpc_server.log")
    
    # log_config.init_logging_stdout()
    
    worker = BrokerWorker('pepe')
    worker.daemon = True
    worker.start()
    
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 9999))
    
    # server.register_introspection_functions()
    server.register_instance(MessagingProxy(worker))
    
    try:
        server.serve_forever()
    except:
        logging.critical("exception", exc_info = True)
    finally:
        worker.shutdown()
        logging.debug("joining")
        worker.join()
    
    log_config.shutdown()
    


if __name__ == '__main__':
    main()


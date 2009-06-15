#!/usr/bin/env python
# encoding: utf-8
"""
dispatcher.py

Created by Brian Lalor on 2009-06-15.
Copyright (c) 2009 __MyCompanyName__. All rights reserved.
"""

## http://effbot.org/zone/thread-synchronization.htm
from __future__ import with_statement

import sys, os
import signal
import time

import SocketServer, socket
import traceback

import threading
import Queue

import xbee, serial
import struct

class XBeeReaderThread(threading.Thread):
    """
    XBee thread.  Reads packets from 
    """
    
    def __init__(self, serial_dev, baud, server):
        threading.Thread.__init__(self, name = "XBee")
        
        self.device = serial_dev
        self.baud = baud
        self.server = server
        self.__shutdown = False
        
    
    
    def run(self):
        ser = serial.Serial(self.device, self.baud)
        
        try:
            while not self.__shutdown:
                packet = xbee.xbee.find_packet(ser)
                
                if packet:
                    self.server.dispatch_packet(packet)
        finally:
            ser.close()
    
    
    def shutdown(self):
        self.__shutdown = True
    


class XBeeDispatcher(SocketServer.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, serial_device, serial_baud):
        self.serial_device = serial_device
        self.serial_baud = serial_baud
        
        SocketServer.ThreadingUnixStreamServer.__init__(self, address, XBeeRequestHandler)

    def server_activate(self):
        self.__shutdown = False
        
        self.__active_clients = []
        self.__active_clients_lock = threading.RLock()
        
        SocketServer.ThreadingUnixStreamServer.server_activate(self)

        ## start XBee communication thread here
        self.xbee_thread = XBeeReaderThread(self.serial_device, self.serial_baud, self)
        self.xbee_thread.start()

    def serve_forever(self):
        while not self.__shutdown:
            self.handle_request()

    
    def server_close(self):
        self.__shutdown = True
        
        SocketServer.ThreadingUnixStreamServer.server_close(self)

        ## close down XBee comm thread
        self.xbee_thread.shutdown()

    def add_active_client(self, client):
        with self.__active_clients_lock:
            if client not in self.__active_clients:
                self.__active_clients.append(client)

    def remove_active_client(self, client):
        with self.__active_clients_lock:
            if client in self.__active_clients:
                self.__active_clients.remove(client)

    def dispatch_packet(self, packet):
        found_clients = False
        
        with self.__active_clients_lock:
            for client in self.__active_clients:
                found_clients = True
                client.enqueue_packet(packet)


        if not found_clients:
            print >>sys.stderr, "no active clients"



class XBeeRequestHandler(SocketServer.BaseRequestHandler):
    def enqueue_packet(self, packet):
        try:
            self.packet_queue.put(packet)
        except Queue.Full:
            print >>sys.stderr, "queue is full!"
    
    def setup(self):
        self.packet_queue = Queue.Queue()
        
        ## add ourselves to the server's list of active clients
        self.server.add_active_client(self)

        ## think this might only work with TCP sockets
        self.request.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
        
    def handle(self):
        ## we *must* ensure that finish() gets called, which only happens when
        ## handle() returns successfully.

        ## at this time, I'm only sending data *to* the clients
        self.request.shutdown(socket.SHUT_RD)
        
        try:
            connection_alive = True
            
            while connection_alive:
                try:
                    # pull a packet from the queue, with a blocking timeout
                    packet = self.packet_queue.get(True, 1)

                    length = len(packet)
                    
                    self.request.send(struct.pack('BH', xbee.xbee.START_IOPACKET, length))
                    self.request.sendall(packet)
                except Queue.Empty:
                    pass
                except socket.error:
                    print >>sys.stderr, "socket error sending packet to client"
                    traceback.print_exc()
                    
                    connection_alive = False


        except:
            print >>sys.stderr, "exception handling request"
            traceback.print_exc()
        
    
    
    def finish(self):
        ## don't do anything too fancy in here; since we're using daemon
        ## threads, this isn't called when the server's shutting down.
        
        ## remove ourselves from the server's list of active clients
        self.server.remove_active_client(self)

        ## close the socket
        self.request.close()


def main():
    server = XBeeDispatcher("socket", sys.argv[1], int(sys.argv[2]))

    # The signals SIGKILL and SIGSTOP cannot be caught, blocked, or ignored.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, lambda signum, frame: server.server_close())
    signal.signal(signal.SIGTERM, lambda signum, frame: server.server_close())
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print >>sys.stderr, "Interrupt caught, shutting down"
    finally:
        print "cleaning up"
        server.server_close()
        os.unlink("socket")


if __name__ == '__main__':
    main()


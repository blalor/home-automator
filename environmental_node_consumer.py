#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import signal
import logging, logging.handlers
import daemonizer

import SimpleXMLRPCServer, threading
import Queue
import random

class EnvironmentalNodeConsumer(consumer.DatabaseConsumer):
    def __init__(self, db_name, xbee_addresses):
        consumer.DatabaseConsumer.__init__(self, db_name, xbee_addresses = xbee_addresses)
        
        self.frame_id = chr(((random.randint(1, 255)) % 255) + 1)
        
        # queue for packets that aren't explicitly handled in handle_packet,
        # so that __send_data can get to them.
        self.extra_messages_queue = Queue.Queue()
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': 'T: 770.42 Cm: 375.14 RH:  42.88 Vcc: 3332 tempC:  19.04 tempF:  66.27\r\n',
        #  'source_addr': '\xda\xe0',
        #  'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xdc'}
        
        now = self.now()
        
        if frame['id'] != 'zb_rx':
            self._logger.error("unhandled frame id %s", frame['id'])
            self.extra_messages_queue.put(frame)
            return
        
        sdata = frame['rf_data'].strip().split()
        
        # self._logger.debug(sdata)
        
        # Parse the sample
        # T: 778.21 Cm: 377.93 RH:  47.17 Vcc: 3342 tempC:  13.31 tempF:  55.96
        if sdata[0] == 'T:':
            rel_humid = float(sdata[5])
            temp_C = float(sdata[9])
            
            # self._logger.debug(str((time.mktime(now.timetuple()), rel_humid, temp_C)))
            
            try:
                self.dbc.execute(
                    "insert into humidity (ts_utc, node_id, rel_humid) values (?, ?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        self._format_addr(frame['source_addr_long']),
                        rel_humid,
                    )
                )
                
                self.dbc.execute(
                    "insert into temperature (ts_utc, node_id, temp_C) values (?, ?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        self._format_addr(frame['source_addr_long']),
                        temp_C,
                    )
                )
            except:
                self._logger.error("unable to insert records to database", exc_info = True)
        else:
            self._logger.error("bad data: %s", unicode(sdata, errors = 'replace'))
    # }}}
    
    # {{{ __checksum
    def __checksum(self, data):
        sum = 0
        for d in data:
            sum ^= ord(d)
            
        return sum
    
    # }}}
    
    # {{{ __build_message
    def __build_message(self, body):
        packet_spec = '<ch%dsB' % (len(body),)
        
        #print packet_spec
        p = struct.pack(packet_spec, '*', len(body), body, checksum(body))
        # print [c for c in p], [hex(ord(c)) for c in p]
        return p
    
    # }}}
    
    # {{{ __send_data
    def __send_data(self, data):
        success = False
        
        # increment frame_id but constrain to 1..255.  Seems to go
        # 2,4,6,…254,1,3,5…255. Whatever.
        self.frame_id = chr(((ord(self.frame_id) + 1) % 255) + 1)
        
        self.xbee.zb_tx_request(frame_id = self.frame_id, dest_addr_long = self.xbee_address, data = data)
        
        while True:
            try:
                frame = self.extra_messages_queue.get(True, 1)
                
                # print frame
                if (frame['id'] == 'zb_tx_status') and \
                   (frame['frame_id'] == self.frame_id):
                    
                    if frame['delivery_status'] == '\x00':
                        # success!
                        success = True
                        self._logger.debug("sent data with %d retries", ord(frame['retries']))
                    else:
                        self._logger.error("send failed after %d retries with status 0x%2X", ord(frame['retries']), ord(frame['status']))
                    
                    break
            except Queue.Empty:
                pass
            
        return success
    
    # }}}
    
    # {{{ read_calibration_data
    def read_calibration_data(self):
        ser.write(self.__build_message('R'))
        data_len = struct.unpack('<h', ser.read(2))[0]
        data = struct.unpack('<7f', ser.read(data_len))

        config = {
            'capacitanceConvFactor': data[0],
            'humiditySensitivity': data[1],
            'temperatureCoefficient': data[2],
            'calibratedHumidity': data[3],
            'calibratedCapacitance': data[4],
            'temperatureSensorCorrectionFactor': data[5],
            'temperatureSensorCorrectionOffset': data[6],
        }
        
    # }}}
    
    # {{{ write_calibration_element
    def write_calibration_element(self):
        pass
    # }}}
    
    # {{{ perform_calibration
    def perform_calibration(self):
        pass
    # }}}
    


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/env_node.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    c = EnvironmentalNodeConsumer(basedir + '/sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:dc', '00:11:22:33:44:55:66:22'])
    xrs = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 10102))
    
    try:
        # fire up XMLRPCServer
        xrs.register_introspection_functions()
        xrs.register_function(c.read_calibration_data, 'read_calibration_data')
        xrs.register_function(c.write_calibration_element, 'write_calibration_element')
        xrs.register_function(c.perform_calibration, 'perform_calibration')
        
        xrs_thread = threading.Thread(target = xrs.serve_forever)
        xrs_thread.daemon = True
        xrs_thread.start()
        
        c.process_forever()
    finally:
        c.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

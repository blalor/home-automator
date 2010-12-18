#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import signal
import logging, logging.handlers
import daemonizer
import threading

class EnvironmentalNodeCalibrator(consumer.DatabaseConsumer):
    START_MARKER = '>>>'
    END_MARKER = '<<<'
    
    def __init__(self, db_name):
        consumer.DatabaseConsumer.__init__(self,
                                           db_name,
                                           socket_dest = ('tonidoplug', 9999),
                                           xbee_addresses = ['00:11:22:33:44:55:66:dc', # T/H Test 2
                                                             '00:11:22:33:44:55:66:22', # T/H Test 1
                                                             '00:11:22:33:44:55:66:da', # temp/humidity calibrator
                                                             '00:11:22:33:44:55:66:7d', # wall router
                                                             '00:11:22:33:44:55:66:a5', # L/T Sensor 
                                                             '00:11:22:33:44:55:66:1d', # T/H Test 3
                                                             ])
        
        self.__buffer = ''
        self.__buffer_lock = threading.RLock()
        
    
    
    # {{{ handle_packet
    def handle_packet(self, frame):
        now = self.now()
        f_addr = self._format_addr(frame['source_addr_long'])
        
        self._logger.debug("packet from %s", f_addr)
        
        # timestamp
        # period = None
        # measured_capacitance = None
        # rel_humid = None
        # temp_C = None
        # channel = f_addr
        data_records = []
        
        if f_addr in ('00:11:22:33:44:55:66:dc', '00:11:22:33:44:55:66:22', '00:11:22:33:44:55:66:1d'):
            if frame['id'] != 'zb_rx':
                self._logger.error("unhandled frame id %s", frame['id'])
                return
            
            # {'id': 'zb_rx',
            #  'options': '\x01',
            #  'rf_data': 'T: 770.42 Cm: 375.14 RH:  42.88 Vcc: 3332 tempC:  19.04 tempF:  66.27\r\n',
            #  'source_addr': '\xda\xe0',
            #  'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xdc'}
            
            sdata = frame['rf_data'].strip().split()
            
            # self._logger.debug(sdata)
            
            # Parse the sample
            # T: 778.21 Cm: 377.93 RH:  47.17 Vcc: 3342 tempC:  13.31 tempF:  55.96
            if sdata[0] == 'T:':
                period = float(sdata[1])
                measured_capacitance = float(sdata[3])
                rel_humid = float(sdata[5])
                temp_C = float(sdata[9])
                
                # period = None
                # measured_capacitance = None
                # rel_humid = None
                # temp_C = None
                # channel = f_addr
                data_records.append((time.mktime(now.timetuple()), period, measured_capacitance, rel_humid, temp_C, f_addr))
            else:
                self._logger.error("bad data: %s", unicode(str(sdata), errors = 'replace'))
            
        elif f_addr in ('00:11:22:33:44:55:66:7d', '00:11:22:33:44:55:66:a5'):
            # wall router or lt sensor
            if frame['id'] != 'zb_rx_io_data':
                self._logger.error("unhandled frame id %s", frame['id'])
                return
            
            if 'samples' not in frame:
                self._logger.error("no samples in frame!")
                return
            
            samples = frame['samples'][0]
            
            if 'adc-2' not in samples:
                self._logger.warn("missing adc-2 sample")
            else:
                temp_C = (self._sample_to_mv(samples['adc-2']) - 500.0) / 10.0
                
                # period = None
                # measured_capacitance = None
                # rel_humid = None
                # temp_C = None
                # channel = f_addr
                data_records.append((time.mktime(now.timetuple()), None, None, None, temp_C, f_addr))
                
            
        elif f_addr == '00:11:22:33:44:55:66:da':
            # the breadboarded XBee
            
            if frame['id'] != 'zb_rx':
                self._logger.error("unhandled frame id %s", frame['id'])
                return
            
            with self.__buffer_lock:
                self.__buffer += frame['rf_data']
                
                if (self.END_MARKER in self.__buffer) and (self.START_MARKER not in self.__buffer):
                    self._logger.error("found end with no start; flushing buffer")
                    self._logger.debug("buffer:\n%s", self.__buffer)
                    self.__buffer = ''
                    return
                
                s_ind = self.__buffer.find(self.START_MARKER) + len(self.START_MARKER)
                e_ind = self.__buffer.find(self.END_MARKER)
                
                if (s_ind != (-1 + len(self.START_MARKER))) and (e_ind != -1):
                    self.__buffer = self.__buffer[s_ind:e_ind]
                    
                    for line in self.__buffer.split('\n'):
                        line = line.strip().split()
                        if not line:
                            continue
                        
                        try:
                            channel = line[0]
                            temp_C = float(line[1])
                            rel_humid = float(line[2])
                        except:
                            self._logger.error("unable to parse line %s", str(line), exc_info = True)
                            self.__buffer = ''
                            return
                        
                        # period = None
                        # measured_capacitance = None
                        # rel_humid = None
                        # temp_C = None
                        # channel = f_addr
                        data_records.append((time.mktime(now.timetuple()), None, None, rel_humid, temp_C, channel))
                
                    self.__buffer = ''
                else:
                    self._logger.debug("match failed on buffer:\n%s", self.__buffer)
            
        else:
            self._logger.error("unhandled device %s", f_addr)
        
        for dr in data_records:
            chan, tc, rh = dr[5], dr[4], dr[3]
            if rh == None:
                rh = -99.99
            
            self._logger.info("(%s) Tc: %5.2f %%RH: %6.2f", chan, tc, rh)
        
        if data_records:
            try:
                self.dbc.executemany(
                    "insert into temp_humid (ts_utc, period, measured_cap, humidity, temp_C, channel) values (?, ?, ?, ?, ?, ?)",
                    data_records
                )
            except:
                self._logger.error("unable to insert records to database", exc_info = True)
        else:
            self._logger.debug("no records in packet from %s", f_addr)
        
        
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/calibration.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    c = EnvironmentalNodeCalibrator(basedir + '/calibration.db')
    
    try:
        c.process_forever()
    finally:
        c.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

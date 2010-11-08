#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import signal
import logging, logging.handlers
import daemonizer

class EnvironmentalNodeCalibrator(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        now = self.now()
        f_addr = self._format_addr(frame['source_addr_long'])
        
        if frame['id'] != 'zb_rx':
            self._logger.error("unhandled frame id %s", frame['id'])
            return
        
        period = None
        measured_capacitance = None
        rel_humid = None
        temp_C = None
        channel = None
        
        if f_addr in ('00:11:22:33:44:55:66:dc', '00:11:22:33:44:55:66:22'):
            channel = f_addr
            
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
            
            else:
                self._logger.error("bad data: %s", unicode(sdata, error = 'replace'))
        
        else:
            # the breadboarded XBee
            
            ## for sketch, test message size against NP, and set RO (packetization timeout) sufficiently high
            
            
        try:
            self.dbc.execute(
                "insert into temp_humid (ts_utc, channel, temp_C, humidity, measured_cap, period) values (?, ?, ?, ?, ?, ?)",
                (
                    time.mktime(now.timetuple()),
                    self._format_addr(frame['source_addr_long']),
                    rel_humid,
                )
            )
        except:
            self._logger.error("unable to insert records to database", exc_info = True)
        
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
    
    c = EnvironmentalNodeCalibrator(basedir + '/calibration.db',
                                    xbee_addresses = ['00:11:22:33:44:55:66:dc', '00:11:22:33:44:55:66:22'])
    
    try:
        c.process_forever()
    finally:
        c.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

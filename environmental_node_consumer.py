#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import signal
import logging, logging.handlers
import daemonizer

class EnvironmentalNodeConsumer(consumer.DatabaseConsumer):
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
            self._logger.error("bad data: %s", unicode(sdata, error = 'replace'))
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/env_node.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    c = EnvironmentalNodeConsumer(basedir + '/sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:dc'])
    
    try:
        c.process_forever()
    finally:
        c.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

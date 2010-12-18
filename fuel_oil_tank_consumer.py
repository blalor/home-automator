#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import logging, logging.handlers
import signal
import daemonizer
import struct

class FuelOilTankConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xcf',
        #  'rf_data': '\x00\x00\x00\x00\n',
        #  'source_addr': '\xeb\x81',
        #  'id': 'zb_rx',
        #  'options': '\x01'}
        
        now = self.now()
        
        if frame['id'] != 'zb_rx':
            self._logger.error("unhandled frame id %s", frame['id'])
            return
        
        # remove trailing whitespace
        data = frame['rf_data'].strip()
        
        if data:
            height = struct.unpack('<f', data)[0]
            
            try:
                self.dbc.execute(
                    "insert into oil_tank (ts_utc, height) values (?, ?)",
                    (
                        time.mktime(now.timetuple()),
                        height,
                    )
                )
            except:
                self._logger.error("unable to insert record into database", exc_info = True)
            
        else:
            self._logger.error("bad data: %s", unicode(data, errors = 'replace'))
    
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/oil_tank.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    fc = FuelOilTankConsumer(basedir + '/sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:cf'])
    
    try:
        fc.process_forever()
    finally:
        fc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

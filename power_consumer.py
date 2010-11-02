#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import consumer
import time
import logging, logging.handlers
import signal
import daemonizer

class PowerConsumer(consumer.DatabaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#23:71#\r\n',
        #  'src_addr': '\x18:',
        #  'src_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        now = self.utcnow()
        
        if frame['id'] != 'zb_rx':
            self._logger.error("unhandled frame id %s", frame['id'])
            return
        
        # #853:0#
        # readings given in amps * 100
        
        data = frame['rf_data'].strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            
            try:
                self.dbc.execute(
                    "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)",
                    (
                        time.mktime(now.utctimetuple()),
                        clamp1,
                        clamp2,
                    )
                )
            except:
                self._logger.error("unable to insert record into database", exc_info = True)
            
        else:
            self._logger.error("bad data: %s", unicode(data, error = 'replace'))
    
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/power.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    pc = PowerConsumer(basedir + '/sensors.db', xbee_addresses = ['00:11:22:33:44:55:66:0a'])
    
    try:
        pc.process_forever()
    finally:
        pc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

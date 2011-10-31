#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import rabbit_consumer as consumer
import logging, logging.handlers
import signal
import daemonizer
import struct

class FuelOilTankConsumer(consumer.BaseConsumer):
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, frame):
        # {'source_addr_long': '\x00\x13\xa2\x00@2\xdc\xcf',
        #  'rf_data': '\x00\x00\x00\x00\n',
        #  'source_addr': '\xeb\x81',
        #  'id': 'zb_rx',
        #  'options': '\x01'}
        
        # remove trailing whitespace
        data = frame['rf_data'].strip()
        
        if data:
            sensor_frame = {
                'timestamp' : frame['_timestamp'],
                'height' : struct.unpack('<f', data)[0],
            }
            
            self.publish_sensor_data('oil_tank', sensor_frame)
        
        else:
            self._logger.error("bad data: %s", unicode(data, errors = 'replace'))
        
        return True
    
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
    
    fc = FuelOilTankConsumer(['00:11:22:33:44:55:66:cf'])
    
    try:
        fc.process_forever()
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        fc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

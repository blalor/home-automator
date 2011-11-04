#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import consumer
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
        
    
    # }}}


def main():
    import log_config, logging
    import signal
    import daemonizer
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/oil_tank.log")
    
    # log_config.init_logging_stdout()
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    fc = FuelOilTankConsumer(['00:11:22:33:44:55:66:cf'])
    
    try:
        fc.process_forever()
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        fc.shutdown()
        log_config.shutdown()
    


if __name__ == '__main__':
    main()

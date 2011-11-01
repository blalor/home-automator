#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# consume raw frames and produce data frames

import sys, os
import logging, logging.handlers
import signal
import daemonizer

import rabbit_consumer as consumer

class PowerConsumer(consumer.BaseConsumer):
    def __init__(self, addr):
        super(PowerConsumer, self).__init__((addr,))
    
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, xbee_frame):
        # {'_timestamp': datetime.datetime(2011, 10, 30, 17, 44, 49, 595486),
        #  'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#107:223#\r\n',
        #  'source_addr': '\x054',
        #  'source_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        sensor_frame = {
            'timestamp' : xbee_frame['_timestamp'],
        }
        
        # #853:0#
        # readings given in amps * 100
        
        data = xbee_frame['rf_data'].strip()
        if data.startswith('#') and data.endswith('#'):
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            
            sensor_frame['clamp1_amps'] = clamp1
            sensor_frame['clamp2_amps'] = clamp2
            
            # publish the sensor data
            self.publish_sensor_data('electric_meter', sensor_frame)
        else:
            self._logger.error("unknown data: %s", data)
        
    
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/power.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(threadName)s] %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    pc = PowerConsumer('00:11:22:33:44:55:66:0a')
    
    try:
        pc.process_forever()
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        pc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

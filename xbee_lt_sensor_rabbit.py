#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# consume raw frames and produce data frames

import sys, os
import logging, logging.handlers
import signal
import daemonizer

import rabbit_consumer as consumer

class LightTempConsumerRabbit(consumer.BaseConsumer):
    def __init__(self, addrs):
        bindings = []
        
        # only listen for IO frames from the XBee light/temp sensors
        for addr in addrs:
            bindings.append("zb_rx_io_data." + addr.lower())
        
        
        super(LightTempConsumerRabbit, self).__init__(bindings)
    
    
    # {{{ handle_packet
    def handle_packet(self, xbee_frame):
        # {'_timestamp': datetime.datetime(2011, 10, 30, 16, 5, 18, 223946),
        #  'id': 'zb_rx_io_data',
        #  'options': '\x01',
        #  'samples': [{'adc-1': 17, 'adc-2': 613, 'dio-0': True}],
        #  'source_addr': 'R\xc1',
        #  'source_addr_long': '\x00\x13\xa2\x00@Un}'}
        
        if 'samples' not in xbee_frame:
            self._logger.error("no samples in frame!")
            return
        
        formatted_addr = self._format_addr(xbee_frame['source_addr_long'])
        
        samples = xbee_frame['samples'][0]
        
        if 'adc-1' not in samples:
            self._logger.warn("missing adc-1 sample")
        else:
            light_sample = {
                'timestamp' : xbee_frame['_timestamp'],
                'node_id'   : formatted_addr,
                'light'     : samples['adc-1']
            }
            
            # publish the sensor data
            self.publish_sensor_data('light.' + formatted_addr, light_sample)
        
        
        if 'adc-2' not in samples:
            self._logger.warn("missing adc-2 sample")
        else:
            temp_C = (self._sample_to_mv(samples['adc-2']) - 500.0) / 10.0
            
            if formatted_addr == "00:11:22:33:44:55:66:7d":
                # router; adjust temp down 4°C
                temp_C -= 4.0
            
            temp_sample = {
                'timestamp' : xbee_frame['_timestamp'],
                'node_id'   : formatted_addr,
                'temp_C'    : temp_C,
                'temp_F'    : (1.8 * temp_C) + 32,
            }
            
            # publish the sensor data
            self.publish_sensor_data('temperature.' + formatted_addr, temp_sample)
            
        
        # humidity = ((sample_to_mv(samples['adc-3']) * 108.2 / 33.2) / 5000.0 - 0.16) / 0.0062
        
        # print '%s sensor reading -- light: %d temp %.1fF/%.1fC' % (formatted_addr, light, temp_F, temp_C)
    
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/xbee_lt_rabbit.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    pc = LightTempConsumerRabbit(('00:11:22:33:44:55:66:a5', '00:11:22:33:44:55:66:7d'))
    
    try:
        pc.process_forever()
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        pc.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

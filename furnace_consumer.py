#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os

import struct

import consumer
import SimpleXMLRPCServer

class FurnaceConsumer(consumer.BaseConsumer):
    zone_states = {
        0 : "unknown",
        1 : "active",
        2 : "inactive",
    }
    
    # {{{ __init__
    def __init__(self, addr):
        self.__xbee_address = addr
        self.__timer_remaining = None
        
        super(FurnaceConsumer, self).__init__([self.__xbee_address])
    
    # }}}
    
    # {{{ __calc_checksum
    def __calc_checksum(self, data):
        chksum = len(data)
        
        for x in data:
            chksum += ord(x)
        
        chksum = (0x100 - (chksum & 0xFF))
        
        return chksum
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, frame):
        data = frame['rf_data']
        
        if data.startswith('\xff\x55'):
            data_len = ord(data[2])
            
            if self.__calc_checksum(data[3:-1]) == ord(data[-1]):
                sample = struct.unpack("<BBBH?HB", data)
                
                self._logger.debug("sample: %s", sample)
                
                zone_state = self.zone_states[sample[3]]
                powered = sample[4]
                self.__timer_remaining = sample[5]
                
                self._logger.debug(
                    "zone %s, powered: %s, time remaining: %d" % (
                        zone_state,
                        str(powered),
                        self.__timer_remaining
                    )
                )
                
                if zone_state != 'unknown':
                    db_zone_val = 0
                    
                    if zone_state == 'active':
                        db_zone_val = 1
                    
                    sensor_frame = {
                        'timestamp' : frame['_timestamp'],
                        'zone_active' : db_zone_val,
                    }
                    
                    self.publish_sensor_data('furnace', sensor_frame)
                    
            else:
                self._logger.warn("bad checksum")
        
    
    # }}}
    
    # {{{ start_timer
    def start_timer(self, duration = 420):
        self._logger.info("starting timer; duration %d", duration)
        self.__timer_remaining = duration
        
        payload = struct.pack("<cH", 'S', duration)
        msg = '\xff\x55%s%s%s' % (
            struct.pack('<B', len(payload)),
            payload,
            struct.pack('<B', self.__calc_checksum(payload))
        )
        
        return self._send_data(self.__xbee_address, msg)
    
    # }}}
    
    # {{{ cancel_timer
    def cancel_timer(self):
        self._logger.info("cancelling timer")
        self.__timer_remaining = 0
        
        payload = struct.pack("<c", 'C')
        msg = '\xff\x55%s%s%s' % (
            struct.pack('<B', len(payload)),
            payload,
            struct.pack('<B', self.__calc_checksum(payload))
        )
        
        return self._send_data(self.__xbee_address, msg)
    
    # }}}
    
    # {{{ get_time_remaining
    def get_time_remaining(self):
        return self.__timer_remaining
    
    # }}}



def main():
    import signal
    import threading
    import daemonizer
    
    import log_config, logging
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/furnace.log")
    
    # log_config.init_logging_stdout()
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    fc = FurnaceConsumer('00:11:22:33:44:55:66:4d')
    xrs = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 10101))
    
    try:
        # fire up XMLRPCServer
        xrs.register_introspection_functions()
        xrs.register_function(fc.start_timer, 'start_timer')
        xrs.register_function(fc.cancel_timer, 'cancel_timer')
        xrs.register_function(fc.get_time_remaining, 'get_time_remaining')
        
        xrs_thread = threading.Thread(target = xrs.serve_forever)
        xrs_thread.daemon = True
        xrs_thread.start()
        
        fc.process_forever()
    except KeyboardInterrupt:
        pass
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        fc.shutdown()
        xrs.shutdown()
        log_config.shutdown()
    


if __name__ == '__main__':
    main()

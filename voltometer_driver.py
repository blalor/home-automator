#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# proxies data from the power monitor to the "volt-o-meter" gauge.

import sys, os
import consumer

import logging, logging.handlers
import signal, threading

import daemonizer
import struct

import SimpleXMLRPCServer

def calc_checksum(data):
    chksum = len(data)
    
    for x in data:
        chksum += ord(x)
    
    chksum = (0x100 - (chksum & 0xFF))
    
    return chksum


def build_packet(name, val):
    payload = struct.pack('<cB', name, val)
    return \
        '\xff\x55' + \
        struct.pack('<B', len(payload)) + \
        payload + \
        struct.pack('<B', calc_checksum(payload))


class VoltometerDriver(consumer.BaseConsumer):
    # the boost converter is only able to supply 32 volts (isn't it actually 34??)
    VOLT_METER_MAX = 32.0
    
    # max observed value from historical data is 92.33; let make the scale a 
    # little (lot) narrower, though
    POWER_METER_MAX = 32.0
    
    def __init__(self, power_mon_addr, voltometer_addr):
        consumer.BaseConsumer.__init__(
            self,
            xbee_addresses = [power_mon_addr, voltometer_addr]
        )
        
        # we only need to concern ourselves with the destination address
        self.voltometer_addr = self.xbee_addresses[1]
    
    
    # {{{ handle_packet
    # parses a packet from the power meter and feeds it to the volt meter
    def handle_packet(self, frame):
        # {'id': 'zb_rx',
        #  'options': '\x01',
        #  'rf_data': '#23:71#\r\n',
        #  'source_addr': '\x18:',
        #  'source_addr_long': '\x00\x13\xa2\x00@:[\n'}
        
        if frame['id'] != 'zb_rx':
            self._logger.debug("unhandled frame id %s", frame['id'])
            return False
        
        # #853:0#
        # readings given in amps * 100
        
        data = frame['rf_data'].strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            
            clamp_tot = clamp1 + clamp2
            
            # range of volt meter is ~0-35, although scale goes to 40
            # conveniently, with the dryer on, the house current draw is about
            # 40…
            
            # scale clamp reading to volt meter scale
            volt_meter_val = (clamp_tot*self.VOLT_METER_MAX)/self.POWER_METER_MAX
            
            # scale volt_meter_val to nearest integer PWM value (0-255), apply correction factor
            pwm_val_raw = (volt_meter_val*255)/self.VOLT_METER_MAX
            
            # constrain value to 255
            pwm_val = min(int(round(pwm_val_raw - (pwm_val_raw * 0.11))), 255)
            
            self._logger.debug(
                'clamp: %.2f, volt meter: %.2f, pwm: %d',
                clamp_tot, volt_meter_val, pwm_val
            )
            
            # kludge to work around bad design in consumer.
            # don't wait for an ack; causing a deadlock while waiting for a 
            # response that won't arrive because we're not consuming responses. :-)
            self._send_data(self.voltometer_addr, build_packet('M', pwm_val), wait_for_ack = False)
            
        else:
            self._logger.error("bad data: %s", unicode(data, errors = 'replace'))
        
        return True
    
    # }}}
    
    # {{{ set_light
    # sets the PWM value for the LED output
    def set_light(self, light_val):
        return self._send_data(self.voltometer_addr, build_packet('L', light_val))
    
    # }}}
    
    # {{{ set_boost
    # sets the PWM value for the boost converter
    def set_boost(self, boost_val):
        return self._send_data(self.voltometer_addr, build_packet('B', boost_val))
    
    # }}}


def main():
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    
    handler = logging.handlers.RotatingFileHandler(basedir + "/logs/voltometer.log",
                                                   maxBytes=(5 * 1024 * 1024),
                                                   backupCount=5)
    
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s -- %(message)s"))
    
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    vd = VoltometerDriver(power_mon_addr = '00:11:22:33:44:55:66:0a', voltometer_addr = '00:11:22:33:44:55:66:e2')
    xrs = SimpleXMLRPCServer.SimpleXMLRPCServer(('', 10104))
    
    try:
        # fire up XMLRPCServer
        xrs.register_introspection_functions()
        xrs.register_function(vd.set_light, 'set_light')
        xrs.register_function(vd.set_boost, 'set_boost')
        
        xrs_thread = threading.Thread(target = xrs.serve_forever)
        xrs_thread.daemon = True
        xrs_thread.start()
        
        vd.process_forever()
        
    except:
        logging.fatal("something bad happened", exc_info = True)
        
    finally:
        vd.shutdown()
        
        xrs.shutdown()
        logging.shutdown()
    


if __name__ == '__main__':
    main()

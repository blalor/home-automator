#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

# proxies data from the power monitor to the "volt-o-meter" gauge.

import sys, os
import threading

import struct

import consumer
import json

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
    
    # {{{ __init__
    def __init__(self, voltometer_addr):
        self.voltometer_addr = voltometer_addr
        
        super(VoltometerDriver, self).__init__([self.voltometer_addr])
        
        self._register_rpc_function('voltometer', self.set_light)
        self._register_rpc_function('voltometer', self.set_boost)
    
    # }}}
    
    # {{{ __handle_meter_packet
    def __handle_meter_packet(self, ch, method, props, body):
        try:
            sensor_frame = json.loads(body)
        
            clamp_tot = sensor_frame['clamp1_amps'] + sensor_frame['clamp2_amps']
        
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
        
            self._send_data(self.voltometer_addr, build_packet('M', pwm_val))
        except:
            self._logger.critical("exception handling meter packet", exc_info = True)
    
    # }}}
    
    # {{{ __run_thread
    def __run_thread(self):
        ## additional AMQP work to subscribe to electric meter messages
        self._sensor_data_conn = self._create_broker_connection()
        self._sensor_data_chan = self._sensor_data_conn.channel()
        
        # create new queue exclusively sensor data messages
        _meter_queue = self._sensor_data_chan.queue_declare(exclusive = True).method.queue
        
        self._sensor_data_chan.basic_consume(self.__handle_meter_packet,
                                             queue = _meter_queue,
                                             no_ack = True)
        
        # listen for electric meter messages
        self._sensor_data_chan.queue_bind(exchange = 'sensor_data',
                                          queue = _meter_queue,
                                          routing_key = 'electric_meter')
        
        self._sensor_data_chan.start_consuming()
        
    
    # }}}
    
    # {{{ process_forever
    def process_forever(self):
        self._logger.debug("starting sens_dat thread")
        t = threading.Thread(target = self.__run_thread, name = 'sens_dat')
        # t.daemon = True
        t.start()
        
        self._logger.debug("invoking parent process_forever")
        
        super(VoltometerDriver, self).process_forever()
        
        self._logger.debug("parent process_forever returned; closing sensor data channel")
        
        self._sensor_data_chan.stop_consuming()
        self._sensor_data_conn.close()
        
        self._logger.debug("process_forever complete")
        
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
    from support import daemonizer, log_config
    import logging
    import signal
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/voltometer.log")
    
    # log_config.init_logging_stdout()
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    vd = VoltometerDriver('00:11:22:33:44:55:66:e2')
    
    try:
        vd.process_forever()
    
    except:
        logging.fatal("something bad happened", exc_info = True)
        
    finally:
        vd.shutdown()
        log_config.shutdown()
    


if __name__ == '__main__':
    main()

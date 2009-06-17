#!/usr/bin/env python
# -*- coding: utf-8 -*-

## my elegant, robust, modular design is too much for the limited memory on
## the WL520gu :-(

import os
import signal
import logging
import xbee, serial
import sqlite3
import datetime, time
import re

class SignalRaised(Exception):
    pass

class MDispatcher(object):
    POWER_ADDR = '0013A2004053A365'
    
    FURNACE_ADDR = '0013A2004053A44D'
    FURNACE_RE = re.compile(r'''^([A-Z][A-Za-z]*)\[(\w+)\]=(-?\d+)$''')

    # {{{ __init__
    def __init__(self, port, baud):
        self.log = logging.getLogger(self.__class__.__name__)
        self.__shutdown = False
        self.__setup_signals()
        
        self.serial = serial.Serial(port = port, baudrate = baud)

        self.dbc = sqlite3.connect('sensors.db',
                                   isolation_level = None,
                                   timeout = 5)

        self.furnace_buf = ''
        self.furnace_found_start = False
        self.furnace_sample_record = {}
    # }}}

    # {{{ __setup_signals
    def __setup_signals(self):
        def sig_hndlr(signum, frame):
            if signum == signal.SIGINT:
                signal.signal(signal.SIGINT, signal.default_int_handler)
            else:
                signal.signal(signum, signal.SIG_DFL)
            
            self.shutdown()
            raise SignalRaised(signum)
        
        signal.signal(signal.SIGQUIT, sig_hndlr)
        signal.signal(signal.SIGTERM, sig_hndlr)
        signal.signal(signal.SIGINT, sig_hndlr)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
    # }}}
    
    # {{{ utcnow
    def utcnow(self):
        return datetime.datetime.utcnow()
    # }}}

    # {{{ shutdown
    def shutdown(self):
        if not self.__shutdown:
            self.__shutdown = True
            self.log.info("shutdown initiated")
            self.serial.close()
            self.dbc.close()
    # }}}
    
    # {{{ handle_furnace
    def handle_furnace(self, xb):
        ## convert the data to chars and append to buffer
        self.furnace_buf = self.furnace_buf + ''.join([chr(c) for c in xb.data])
        
        ## process each line found in the buffer
        while self.furnace_buf.count('\n') > 0:
            eol_ind = self.furnace_buf.index('\n')
            line = self.furnace_buf[:eol_ind + 1].strip()
            
            ## strip off the line we've just found
            self.furnace_buf = self.furnace_buf[eol_ind + 1:]
            
            if line == "<begin>":
                if self.furnace_found_start:
                    self.log.error("found <begin> without <end>; discarding collected data")
                
                self.furnace_found_start = True
                
                # default to unknown values
                self.furnace_sample_record = {
                    'Tc:sophies_room' : None,
                    'Tc:living_room'  : None,
                    'Tc:outside'      : None,
                    'Tc:basement'     : None,
                    'Tc:master'       : None,
                    'Tc:office'       : None,
                    'Z:master'        : None,
                    'Z:living_room'   : None,
                }
            
            elif line == "<end>":
                # finalize
                if not self.furnace_found_start:
                    self.log.error("found <end> without <begin>; discarding collected data")
                    
                else:
                    now = self.utcnow()
        
                    for k in self.furnace_sample_record.keys():
                        if self.furnace_sample_record[k] == None:
                            self.log.error("missing value for %s" % (k,))
                            
                    try:
                        self.dbc.execute(
                            """
                            insert into room_temp (
                                ts_utc, sophies_room, living_room, outside,
                                basement, master, office,
                                master_zone, living_room_zone
                            ) values (
                                ?, ?, ?, ?,
                                ?, ?, ?,
                                ?, ?)
                            """,
                            (
                                time.mktime(now.timetuple()),
                                self.furnace_sample_record['Tc:sophies_room'],
                                self.furnace_sample_record['Tc:living_room'],
                                self.furnace_sample_record['Tc:outside'],
                                self.furnace_sample_record['Tc:basement'],
                                self.furnace_sample_record['Tc:master'],
                                self.furnace_sample_record['Tc:office'],
                                self.furnace_sample_record['Z:living_room'],
                                self.furnace_sample_record['Z:master'],
                            )
                        )
                    except:
                        self.log.critical("unable to insert row for room_temp", exc_info = True)
                    
                self.furnace_found_start = False
            else:
                match = self.FURNACE_RE.match(line)
                if not match:
                    self.log.warn("cannot match " + line)
                    continue
                    
                sample_type, sample_label, sample_value = match.groups()
                sample_value = int(sample_value)
                
                if sample_type == 'Tc':
                    # convert from int/long value sent from Arduino to °C
                    sample_value /= 10000.0
                    
                    # oh, what the hell. put °F in there, too
                    self.furnace_sample_record['Tf:' + sample_label] = (sample_value * 1.8) + 32.0
                    
                    
                self.furnace_sample_record[sample_type + ':' + sample_label] = sample_value
    # }}}

    # {{{ handle_power
    def handle_power(self, xb):
        # #853:0#
        # readings given in amps * 100
        
        data = ''.join([chr(c) for c in xb.data]).strip()
        if data.startswith('#') and data.endswith('#'):
            
            clamp1, clamp2 = [int(c)/100.0 for c in data[1:-1].split(":")]
            self.log.debug("clamp 1 %fA, clamp 2 %fA" % (clamp1, clamp2))
            
            try:
                self.dbc.execute(
                    "insert into power (ts_utc, clamp1, clamp2) values (?, ?, ?)",
                    (
                        time.mktime(self.utcnow().timetuple()),
                        clamp1,
                        clamp2,
                    )
                )
            except:
                self.log.critical("unable to insert row into power", exc_info = True)
        else:
            self.log.warn("bad data: " + data)
    # }}}

    # {{{ dispatch
    def dispatch(self):
        while not self.__shutdown:
            try:
                packet = xbee.xbee.find_packet(self.serial)
                
                if packet:
                    if packet.address_64 == self.FURNACE_ADDR:
                        self.handle_furnace(packet)
                    elif packet.address_64 == self.POWER_ADDR:
                        self.handle_power(packet)
                    else:
                        self.log.warn("Unhandled packet: %s" % (str(packet),))
            except SignalRaised, e:
                # just break out of processing
                self.log.info("interrupted by signal %d" % (e.args[0],))

        self.log.info("we've been shut down!")
    # }}}

def main():
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s -- %(message)s',
                        filename='dispatcher.log',
                        filemode='a')

    logging.info("starting up in %s" % (os.getcwd(),))

    md = None
    
    try:
        md = MDispatcher("/dev/tts/0", 115200)
        md.dispatch()
    except:
        logging.critical("unhandled exception", exc_info = True)
    finally:
        if md != None:
            md.shutdown()

        logging.info("exiting")
        logging.shutdown()
    

if __name__ == '__main__':
    main()

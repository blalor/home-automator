#!/usr/bin/env python2.6
# encoding: utf-8
"""
xbee_device_config.py

Created by Brian Lalor on 2010-10-30.
Copyright (c) 2010 __MyCompanyName__. All rights reserved.
"""

import sys
import getopt

import consumer
import threading
import logging

import logging

from pprint import pprint

help_message = '''

    querying:
        --query-all=<long_address> : queries all parameters of the device
    
    setting:
        --config-file=<name of module in path>
        --set=<long address> : sets parameters for a single device
        --set-all : sets parameters for all configured devices
        
        --point-to-me : sets DH/DL for the target device(s)
        --write : writes data to NVRAM
'''

ALL_PARAMETERS = (
    # addressing:
    'DH',
    'DL',
    'MY',
    'MP',
    'NC',
    'SH',
    'SL',
    'NI',
    'SE',
    'DE',
    'CI',
    'NP',
    'DD',
    
    # networking:
    'CH',
    'ID',
    'OP',
    'NH',
    'BH',
    'OI',
    'NT',
    'NO',
    'SC',
    'SD',
    'ZS',
    'NJ',
    'JV',
    'NW',
    'JN',
    'AR',
    
    # security:
    'EE',
    'EO',
    'NK',
    # 'KY', write only
    
    # rf interfacing:
    'PL',
    'PM',
    'DB',
    'PP',
    
    # serial interfacing (i/o):
    'AP',
    'AO',
    'BD',
    'NB',
    'SB',
    'RO',
    'D7',
    'D6',
    
    # i/o commands:
    'IR',
    'IC',
    'P0',
    'P1',
    'P2',
    'P3',
    'D0',
    'D1',
    'D2',
    'D3',
    'D4',
    'D5',
    'D8',
    'LT',
    'PR',
    'RP',
    '%V',
    'V+',
    'TP',
    
    # diagnostics:
    'VR',
    'HV',
    'AI',
    
    # at commands:
    'CT',
    'GT',
    'CC',
    
    # sleep commands:
    'SM',
    'SN',
    'SP',
    'ST',
    'SO',
    'WH',
    'PO',
)

class Usage(Exception):
    pass


class Gateway(consumer.BaseConsumer):
    def __init__(self):
        super(Gateway, self).__init__('ALL')
    
    
    def handle_packet(self, formatted_addr, packet):
        pass
    
    
    def remote_at(self, dest, command, param_val = None):
        return self._send_remote_at(dest, command, param_val, give_me_the_frame = True)
    


def main(argv=None):
    if argv is None:
        argv = sys.argv
    
    set_addr = None
    set_all = False
    config_file = None
    point_to_me = False
    write_to_nvram = False
    
    query_addr = None
    
    try:
        try:
            opts, args = getopt.getopt(
                argv[1:],
                "hS:sQ:f:H:P:",
                ["help",
                 "set=",
                 "set-all",
                 "query-all=",
                 "config-file=",
                 "point-to-me",
                 "write"]
            )
        except getopt.error, msg:
            raise Usage(msg)
        
        # option processing
        for option, value in opts:
            if option in ("-h", "--help"):
                raise Usage(help_message)
            
            if option in ("--set", "-S"):
                set_addr = value
            
            if option in ("--set-all", "-s"):
                set_all = True
            
            if option in ("--query-all", "-Q"):
                query_addr = value
            
            if option in ("--config-file", "-f"):
                config_file = value
            
            if option in ("--point-to-me",):
                point_to_me = True
            
            if option in ("--write",):
                write_to_nvram = True
            
        
        if (set_addr != None) and set_all:
            raise Usage("set and set-all are mutually exclusive")
        
        if (set_addr != None) or set_all:
            if query_addr != None:
                raise Usage("query and set operations are mutually exclusive")
            
            if config_file == None:
                raise Usage("must have a config file to set parameters")
        elif query_addr == None:
            raise Usage("must select a set or query command")
        
        
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.message)
        print >> sys.stderr, "\t for help use --help"
        return 2
    
    
    gw = Gateway()
    
    gw_thread = threading.Thread(target = gw.process_forever, name = "gw_proc")
    gw_thread.daemon = True
    gw_thread.start()
    
    logging.debug("waiting for consumer to be ready")
    gw.ready_event.wait()
    logging.info("ready")
    
    if (set_addr != None) or set_all:
        CONFIG_DATA = __import__(config_file).CONFIG_DATA
        
        if set_all:
            sections = CONFIG_DATA.keys()
        else:
            sections = [set_addr]
        
        SH, SL = None, None
        
        if point_to_me:
            # retrieve our SH and SL
            
            # @todo this isn't really correct; address is for the coordinator,
            # not the transmitting device
            SH = gw.remote_at("00:00:00:00:00:00:00:00", "SH")['parameter']
            SL = gw.remote_at("00:00:00:00:00:00:00:00", "SL")['parameter']
        
        for addr in sections:
            logging.info("configuring %s", addr)
            
            if point_to_me:
                CONFIG_DATA[section]['DH'] = SH
                CONFIG_DATA[section]['DL'] = SL
            
            # need AC as the last command, to make it take effect
            for opt in CONFIG_DATA[section].keys():
                val = CONFIG_DATA[section][opt]
                
                logging.debug(str((section, opt, val)))
                
                resend = True
                while resend:
                    frame = gw.remote_at(section, opt, param_val = val)
                    
                    logging.debug(str(frame))
                    
                    if frame['status'] == '\x00':
                        resend = False
                        logging.info("set %s = '%s'", opt, unicode(val, errors='replace'))
                    elif frame['status'] == '\x04':
                        logging.warn("timeout setting %s", opt)
                    else:
                        resend = False
                        logging.error("error setting %s: %02X", opt, ord(frame['status']))
                    
                    break
            
            logging.debug('sending "AC"')
            
            resend = True
            while resend:
                frame = gw.remote_at(section, 'AC')
                
                logging.debug(str(frame))
                
                if frame['status'] == '\x00':
                    resend = False
                    logging.info("changes applied")
                elif frame['status'] == '\x04':
                    logging.warn("timeout sending AC")
                else:
                    resend = False
                    logging.error("error sending AC: %02X", ord(frame['status']))
                
                break
            
            if not write_to_nvram:
                logging.warn("not writing changes")
            else:
                logging.debug('writing changes')
                
                resend = True
                while resend:
                    frame = gw.remote_at(section, 'WR')
                    
                    logging.debug(str(frame))
                    
                    if frame['status'] == '\x00':
                        resend = False
                        logging.info("changes written")
                    elif frame['status'] == '\x04':
                        logging.warn("timeout sending WR")
                    else:
                        resend = False
                        logging.error("error sending WR: %02X", ord(frame['status']))
                    break
            
            
        
    else:
        # query
        addr = query_addr
        
        data = {}
        for opt in ALL_PARAMETERS:
            opt = opt.upper()
            # print section, opt
            
            resend = True
            while resend:
                frame = gw.remote_at(addr, opt)
                
                if frame['status'] == '\x00':
                    resend = False
                    if 'parameter' in frame:
                        val = frame['parameter']
                        # val = "0x" + "".join('%02X' % ord(x) for x in frame['parameter'])
                        # print query_addr, opt, " ".join('%02X' % ord(x) for x in frame['parameter'])
                        # print opt, val #, "(%d)" % int(val, 16)
                        data[opt] = val
                    else:
                        logging.error("no parameter in frame for command %s! %s", opt, str(frame))
                elif frame['status'] == '\x04':
                    logging.warn("timeout querying %s", opt)
                else:
                    resend = False
                    # logging.error("error querying %s: %02X", opt, ord(frame['status']))
                    # logged by BaseConsumer
                    
                break
                    
        pprint(data)
    
    logging.debug("cleaning up")
    gw.shutdown()
    gw_thread.join()
    logging.shutdown()
    


if __name__ == "__main__":
    from support import log_config
    
    log_config.init_logging_stdout(level = logging.INFO)
    
    sys.exit(main())

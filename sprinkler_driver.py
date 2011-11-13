#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-

import sys, os
import consumer

class InvalidSprinkler(Exception):
    pass


class SprinklerConsumer(consumer.BaseConsumer):
    # mapping of sprinkler ID to DIO config command
    sprinkler_map = {
        1 : 'D0',
        2 : 'D1',
    }
    
    # {{{ __init__
    def __init__(self, xbee_address):
        self.xbee_address = xbee_address
        
        self.__sprinkler_active = {
            1 : False,
            2 : False,
        }
        
        super(SprinklerConsumer, self).__init__([self.xbee_address])
        
        self._register_rpc_function('sprinkler', self.activate_sprinkler)
        self._register_rpc_function('sprinkler', self.deactivate_sprinkler)
        self._register_rpc_function('sprinkler', self.sprinkler_active)
    
    # }}}
    
    # {{{ handle_packet
    def handle_packet(self, formatted_addr, frame):
        self._logger.info("handle_packet not implemented handling %s", frame)
    
    # }}}
    
    # {{{ activate_sprinkler
    def activate_sprinkler(self, sprinkler_id):
        if sprinkler_id not in self.sprinkler_map:
            raise InvalidSprinkler("invalid sprinkler id %s" % (str(sprinkler_id),))
        
        self._logger.info("activating sprinkler %s", sprinkler_id)
        
        success = False
        
        dio_cmd = self.sprinkler_map[sprinkler_id]
        
        if self._send_remote_at(self.xbee_address, command = dio_cmd, param_val = '\x05'):
            success = self._send_remote_at(self.xbee_address, command = 'AC')
        
        if success:
            self.__sprinkler_active[sprinkler_id] = True;
        
        return success
    
    # }}}
    
    # {{{ deactivate_sprinkler
    def deactivate_sprinkler(self, sprinkler_id):
        if sprinkler_id not in self.sprinkler_map:
            raise InvalidSprinkler("invalid sprinkler id %s" % (str(sprinkler_id),))
        
        self._logger.info("deactivating sprinkler %s", sprinkler_id)
        
        success = False
        
        dio_cmd = self.sprinkler_map[sprinkler_id]
        
        if self._send_remote_at(self.xbee_address, command = dio_cmd, param_val = '\x04'):
            success = self._send_remote_at(self.xbee_address, command = 'AC')
        
        if success:
            self.__sprinkler_active[sprinkler_id] = False;
        
        return success
    
    # }}}
    
    # {{{ sprinkler_active
    def sprinkler_active(self, sprinkler_id):
        return self.__sprinkler_active[sprinkler_id]
    
    # }}}


def main():
    from support import daemonizer, log_config
    import logging
    import signal
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    daemonizer.createDaemon()
    log_config.init_logging(basedir + "/logs/sprinkler.log")
    
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    
    sc = SprinklerConsumer('00:11:22:33:44:55:66:1d')
    
    try:
        sc.process_forever()
    except:
        logging.error("unhandled exception", exc_info=True)
    finally:
        sc.shutdown()
        log_config.shutdown()
    


if __name__ == '__main__':
    main()

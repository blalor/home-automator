"""
impl.py

By Paul Malmsten, 2010
Inspired by code written by Amit Synderman and Marco Sangalli
pmalmsten@gmail.com

This module implements an XBee Series 1/Series 2 driver.
"""
import struct
from xbee.base import XBeeBase

class XBee(XBeeBase):
    """
    Provides an implementation of the XBee API for Series 1/2 modules
    with recent firmware.
    
    Commands may be sent to a device by instansiating this class with
    a serial port object (see PySerial) and then calling the send
    method with the proper information specified by the API. Data may
    be read from a device (syncronously only, at the moment) by calling 
    wait_read_frame.
    """
    # Packets which can be sent to an XBee
    
    # Format: 
    #        {name of command:
    #           [{name:field name, len:field length, default: default value sent}
    #            ...
    #            ]
    #         ...
    #         }
    api_commands = {"tx_long_addr":
                        [{'name':'id',              'len':1,        'default':'\x00'},
                         {'name':'frame_id',        'len':1,        'default':'\x00'},
                         {'name':'dest_addr',       'len':8,        'default':None},
                         {'name':'options',         'len':1,        'default':'\x00'},
                         {'name':'data',            'len':None,     'default':None}],
                    "tx":
                        [{'name':'id',              'len':1,        'default':'\x01'},
                         {'name':'frame_id',        'len':1,        'default':'\x00'},
                         {'name':'dest_addr',       'len':2,        'default':None},
                         {'name':'options',         'len':1,        'default':'\x00'},
                         {'name':'data',            'len':None,     'default':None}],
                    "at":
                        [{'name':'id',              'len':1,        'default':'\x08'},
                         {'name':'frame_id',        'len':1,        'default':'\x00'},
                         {'name':'command',         'len':2,        'default':None},
                         {'name':'parameter',       'len':None,     'default':None}],
                    "queued_at":
                        [{'name':'id',              'len':1,        'default':'\x09'},
                         {'name':'frame_id',        'len':1,        'default':'\x00'},
                         {'name':'command',         'len':2,        'default':None},
                         {'name':'parameter',       'len':None,     'default':None}],
                    "zb_tx_request":
                       [{'name':'id',               'len':1,        'default':'\x10'},
                        {'name':'frame_id',         'len':1,        'default':'\x00'},
                        {'name':'dest_addr_long',   'len':8,        'default':struct.pack('>Q', 0)},
                        {'name':'dest_addr',        'len':2,        'default':'\xFF\xFE'},
                        {'name':'bcast_radius',     'len':1,        'default':'\x00'},
                        {'name':'options',          'len':1,        'default':'\x00'},
                        {'name':'data',             'len':None,     'default':None}],
                    "zb_explicit_tx_request":
                       [{'name':'id',               'len':1,        'default':'\x11'},
                        {'name':'frame_id',         'len':1,        'default':'\x00'},
                        {'name':'dest_addr_long',   'len':8,        'default':struct.pack('>Q', 0)},
                        {'name':'dest_addr',        'len':2,        'default':'\xFF\xFE'},
                        {'name':'source_endpoint',     'len':1,        'default':'\x00'},
                        {'name':'dest_endpoint',    'len':1,        'default':'\x00'},
                        {'name':'cluster_id',       'len':2,        'default':'\x00'},
                        {'name':'profile_id',       'len':2,        'default':'\x00'},
                        {'name':'bcast_radius',     'len':1,        'default':'\x00'},
                        {'name':'options',          'len':1,        'default':'\x00'},
                        {'name':'data',             'len':None,     'default':None}],
                    "remote_at":
                        [{'name':'id',              'len':1,        'default':'\x17'},
                         {'name':'frame_id',        'len':1,        'default':'\x00'},
                         # dest_addr_long is 8 bytes (64 bits), so use an unsigned long long
                         {'name':'dest_addr_long',  'len':8,        'default':struct.pack('>Q', 0)},
                         {'name':'dest_addr',       'len':2,        'default':'\xFF\xFE'},
                         {'name':'options',         'len':1,        'default':'\x02'},
                         {'name':'command',         'len':2,        'default':None},
                         {'name':'parameter',       'len':None,     'default':None}],
                    }
    
    # Packets which can be received from an XBee
    
    # Format: 
    #        {id byte received from XBee:
    #           {name: name of response
    #            structure:
    #                [ {'name': name of field, 'len':length of field}
    #                  ...
    #                  ]
    #            parse_as_io_samples:name of field to parse as io
    #           }
    #           ...
    #        }
    #
    api_responses = {"\x80":
                        {'name':'rx_long_addr',
                         'structure':
                            [{'name':'source_addr', 'len':8},
                             {'name':'rssi',        'len':1},
                             {'name':'options',     'len':1},
                             {'name':'rf_data',     'len':None}]},
                     "\x81":
                        {'name':'rx',
                         'structure':
                            [{'name':'source_addr', 'len':2},
                             {'name':'rssi',        'len':1},
                             {'name':'options',     'len':1},
                             {'name':'rf_data',     'len':None}]},
                     "\x82":
                        {'name':'rx_io_data_long_addr',
                         'structure':
                            [{'name':'source_addr_long','len':8},
                             {'name':'rssi',            'len':1},
                             {'name':'options',         'len':1},
                             {'name':'samples',         'len':None}],
                         'parse_as_io_samples':'samples'},
                     "\x83":
                        {'name':'rx_io_data',
                         'structure':
                            [{'name':'source_addr', 'len':2},
                             {'name':'rssi',        'len':1},
                             {'name':'options',     'len':1},
                             {'name':'samples',     'len':None}],
                         'parse_as_io_samples':'samples'},
                     "\x88":
                        {'name':'at_response',
                         'structure':
                            [{'name':'frame_id',    'len':1},
                             {'name':'command',     'len':2},
                             {'name':'status',      'len':1},
                             {'name':'parameter',   'len':None}]},
                     "\x89":
                        {'name':'tx_status',
                         'structure':
                            [{'name':'frame_id',    'len':1},
                             {'name':'status',      'len':1}]},
                     "\x8A":
                        {'name':'status',
                         'structure':
                            [{'name':'status',      'len':1}]},
                     "\x8B":
                        {'name': 'zb_tx_status',
                         'structure':
                            [{'name':'dest_addr',        'len':2},
                             {'name':'retries',          'len':1},
                             {'name':'delivery_status',  'len':1},
                             {'name':'discovery_status', 'len':1}]},
                     "\x90":
                        {'name': 'zb_rx',
                         'structure':
                            [{'name':'source_addr_long', 'len':8},
                             {'name':'source_addr',      'len':2},
                             {'name':'options',          'len':1},
                             {'name':'rf_data',          'len':None}]},
                     "\x91":
                        {'name': 'zb_explicit_rx',
                         'structure':
                            [{'name':'source_addr_long', 'len':8},
                             {'name':'source_addr',      'len':2},
                             {'name':'source_endpoint',  'len':1},
                             {'name':'dest_endpoint',    'len':1},
                             {'name':'cluster_id',       'len':2},
                             {'name':'profile_id',       'len':2},
                             {'name':'options',          'len':1},
                             {'name':'rf_data',          'len':None}]},
                     "\x92":
                        {'name': 'zb_rx_io_data',
                         'structure':
                            [{'name':'source_addr_long', 'len':8},
                             {'name':'source_addr',      'len':2},
                             {'name':'options',          'len':1},
                             {'name':'samples',          'len':None}],
                         'parse_as_zb_io_samples':'samples'},
                     "\x94":
                        {'name': 'zb_rx_sensor_read',
                         'structure':
                            [{'name':'source_addr_long', 'len':8},
                             {'name':'source_addr',      'len':2},
                             {'name':'options',          'len':1},
                             {'name':'1-wire_sensors',   'len':1},
                             {'name':'data',             'len':None},
                             # {'name':'adc_values',       'len':8},
                             # {'name':'temperature',      'len':2},
                         ]},
                     "\x95":
                        {'name': 'zb_node_ident',
                         'structure':
                            [{'name':'source_addr_long', 'len':8},
                             {'name':'source_addr',      'len':2},
                             {'name':'options',          'len':1},
                             {'name':'remote_addr',      'len':2},
                             {'name':'remote_addr_long', 'len':8},
                             {'name':'ni_str',           'len':'null_terminated_string'},
                             {'name':'parent_addr',      'len':2},
                             {'name':'device_type',      'len':1},
                             {'name':'source_event',     'len':1},
                             {'name':'profile_id',       'len':2},
                             {'name':'mfg_id',           'len':2}]},
                    "\x97":
                        {'name':'remote_at_response',
                         'structure':
                            [{'name':'frame_id',        'len':1},
                             {'name':'source_addr_long','len':8},
                             {'name':'source_addr',     'len':2},
                             {'name':'command',         'len':2},
                             {'name':'status',          'len':1},
                             {'name':'parameter',       'len':None}]},
                     }
    
    def __init__(self, *args, **kwargs):
        # Call the super class constructor to save the serial port
        super(XBee, self).__init__(*args, **kwargs)

    def __del__(self):
        # Call the super class destructor to safely shutdown
        super(XBee, self).__del__()

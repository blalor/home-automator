#!/usr/bin/env python
# encoding: utf-8
"""
declare_exchanges.py

Created by Brian Lalor on 2011-11-12.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os
import pika
from config import config_data as config

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config.message_broker.host)
    )
    
    channel = connection.channel()
    
    for exchg in config.message_broker.exchanges:
        exchg_data = config.message_broker.exchanges[exchg]
        
        print "declaring exchange", exchg
        
        channel.exchange_declare(exchange = exchg, type = exchg_data.type)
    
    connection.close()
    


if __name__ == '__main__':
    main()


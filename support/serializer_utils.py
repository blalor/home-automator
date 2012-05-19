#!/usr/bin/env python
# encoding: utf-8
"""
serializer_utils.py

Created by Brian Lalor on 2012-05-18.
"""

import cPickle as pickle
import json
import bson

import datetime, time

import time_util
SYSTEM_TZ = time_util.SYSTEM_TZ
UTC = time_util.UTC

CONTENT_TYPE_PICKLE = 'application/x-python-pickle'
CONTENT_TYPE_JSON   = 'application/json'
CONTENT_TYPE_BSON   = 'application/bson'

## alias to affect all uses
CONTENT_TYPE_BINARY = CONTENT_TYPE_BSON


class InvalidSerializationContentType(Exception):
    pass


def _dthandler(obj):
    if isinstance(obj, datetime.datetime):
        dt = obj
        
        if dt.tzinfo == None:
            # naive
            dt = SYSTEM_TZ.localize(obj)
        
        return dt.astimezone(UTC).isoformat()
    
    elif isinstance(obj, Exception):
        return {
            'py/class': obj.__class__.__name__,
            'args': obj.args,
            'message': obj.message
        }
    else:
        raise TypeError(repr(obj) + " is not JSON serializable")



# {{{ serialize
def serialize(data, content_type):
    retVal = None

    if content_type == CONTENT_TYPE_PICKLE:
        retVal = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

    elif content_type == CONTENT_TYPE_JSON:
        # serializes an object to JSON, with handling for datetime instances
        retVal = json.dumps(data, default = _dthandler)
    
    elif content_type == CONTENT_TYPE_BSON:
        retVal = bson.dumps(data)

    else:
        raise InvalidSerializationContentType(content_type)

    return retVal

# }}}

# {{{ deserialize
def deserialize(data, content_type):
    retVal = None

    if content_type == CONTENT_TYPE_PICKLE:
        retVal = pickle.loads(data)
    
    elif content_type == CONTENT_TYPE_JSON:
        # @todo turn timestamp into datetime
        retVal = json.loads(data)

    elif content_type == CONTENT_TYPE_BSON:
        retVal = bson.loads(data)
    
    else:
        raise InvalidSerializationContentType(content_type)

    return retVal

# }}}

if __name__ == '__main__':
    import iso8601
    import bson

    now = bson.loads(bson.dumps({'now': SYSTEM_TZ.localize(datetime.datetime.now())}))['now']
    utcnow = datetime.datetime.utcnow()

    ser = _dthandler(now)
    utcser = _dthandler(utcnow)

    print ">>> now"
    print "         naive:", now.isoformat()
    print "    serialized:", ser
    print "iso8601 parsed:", iso8601.parse_date(ser).isoformat()
    print "   iso8601 sys:", iso8601.parse_date(ser).astimezone(SYSTEM_TZ).isoformat()


    print ">>> utcnow"
    print "         naive:", utcnow.isoformat()
    print "    serialized:", utcser
    print "iso8601 parsed:", iso8601.parse_date(utcser).isoformat()
    print "   iso8601 sys:", iso8601.parse_date(utcser).astimezone(SYSTEM_TZ).isoformat()

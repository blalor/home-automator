#!/usr/bin/env python
# encoding: utf-8
"""
serializer_utils.py

Created by Brian Lalor on 2012-05-18.
"""

import cPickle as pickle
import json
import bson

import datetime, time, pytz

CONTENT_TYPE_PICKLE = 'application/x-python-pickle'
CONTENT_TYPE_JSON   = 'application/json'
CONTENT_TYPE_BSON   = 'application/bson'

__SYSTEM_TZ = pytz.timezone(time.tzname[0])

class InvalidSerializationContentType(Exception):
    pass


# {{{ serialize
def serialize(data, content_type):
    retVal = None

    if content_type == CONTENT_TYPE_PICKLE:
        retVal = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

    elif content_type == CONTENT_TYPE_JSON:
        # serializes an object to JSON, with handling for datetime instances
        
        def dthandler(obj):
            if isinstance(obj, datetime.datetime):
                dt = obj
                
                if dt.tzinfo == None:
                    # naive
                    dt = __SYSTEM_TZ.localize(obj)
                
                return dt.astimezone(pytz.utc).isoformat()
            
            elif isinstance(obj, Exception):
                return {
                    'py/class': obj.__class__.__name__,
                    'args': obj.args,
                    'message': obj.message
                }
            else:
                raise TypeError(repr(obj) + " is not JSON serializable")
        
        retVal = json.dumps(data, default=dthandler)
    
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

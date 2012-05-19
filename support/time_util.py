#!/usr/bin/env python
# encoding: utf-8
"""
time_util.py

Created by Brian Lalor on 2011-11-09.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import time
from datetime import datetime

import pytz

# cannot use time.tzname[0]
# https://bugs.launchpad.net/pytz/+bug/1001631
SYSTEM_TZ = pytz.timezone('US/Eastern')
UTC = pytz.utc # used by others

def dt_from_epoch_as_system_tz(epoch):
    return SYSTEM_TZ.localize(datetime.fromtimestamp(epoch))


def dt_from_epoch_with_tz(epoch, tz_name):
    return pytz.timezone(tz_name).localize(datetime.fromtimestamp(epoch))


def main():
    pass


if __name__ == '__main__':
    main()


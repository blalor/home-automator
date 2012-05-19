#!/usr/bin/env python
# encoding: utf-8
"""
wx_publisher.py

Created by Brian Lalor on 2011-11-09.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import json, urllib2
from support import time_util
import json

import pika

from config import config_data as config

def fcttime_to_datetime(fcttime):
    return time_util.dt_from_epoch_as_system_tz(int(fcttime['epoch']))
    

    
def _float(f):
    retVal = None
    
    try:
        retVal = float(f)
    except ValueError:
        pass
    
    return retVal


def publish_hourly_forecast(data, ch):
    hourly_forecast = []
    for hf in data['hourly_forecast']:
        hf['FCTTIME'] = fcttime_to_datetime(hf['FCTTIME'])
        
        hourly_forecast.append({
            'timestamp'     : hf['FCTTIME'],
            'condition'     : (hf['condition'], hf['fctcode']),
            'temp_C'        : _float(hf['temp']['metric']),
            'dewpoint_C'    : _float(hf['dewpoint']['metric']),
            'windchill_C'   : _float(hf['windchill']['metric']),
            'rel_humid'     : _float(hf['humidity']),
            'windspeed_mph' : _float(hf['wspd']['english']),
            'wind_dir'      : _float(hf['wdir']['degrees']),
            'pct_of_precip' : _float(hf['pop']),
            'qpf_mm'        : _float(hf['qpf']['metric']),
            'snow_mm'       : _float(hf['snow']['metric']),
            'sky'           : _float(hf['sky']),
            'uvi'           : _float(hf['uvi']),
        })
        
    # publish to wx.hourly_forecast
    ch.basic_publish(
        exchange='weather',
        routing_key='hourly_forecast',
        properties = pika.BasicProperties(
            content_type = 'application/json'
        ),
        body=json.dumps(hourly_forecast, default=time_util.json_date_handler)
    )


def publish_current_observation(data, ch):
    co = data['current_observation']
    
    current_obs = {
        'timestamp'     : time_util.dt_from_epoch_with_tz(int(co['local_epoch']), co['local_tz_long']),
        'station_id'    : co['station_id'],
        'temp_C'        : co['temp_c'],
        'windchill_C'   : _float(co['windchill_c']),
        'dewpoint_C'    : co['dewpoint_c'],
        'rel_humid'     : _float(co['relative_humidity'].replace("%", "")),
        'windspeed_mph' : co['wind_mph'],
        'windgust_mph'  : co['wind_gust_mph'],
        'wind_dir'      : co['wind_degrees'],
        'pressure_mb'   : _float(co['pressure_mb']),
        'visibility_mi' : _float(co['visibility_mi']),
        'weather'       : co['weather'],
        'precip_mm'     : {
            '1hr'   : _float(co['precip_1hr_metric']),
            'today' : _float(co['precip_today_metric']),
        },
    }
    
    # publish to wx.current_observation
    ch.basic_publish(
        exchange='weather',
        routing_key='current_observation',
        properties = pika.BasicProperties(
            content_type = 'application/json'
        ),
        body=json.dumps(current_obs, default=time_util.json_date_handler)
    )


def publish_7day_forecast(data, ch):
    daily_forecast = []
    for df in data['forecast']['simpleforecast']['forecastday']:
        daily_forecast.append({
            'timestamp'     : time_util.dt_from_epoch_with_tz(
                int(df['date']['epoch']),
                df['date']['tz_long']
            ),
            'condition'     : (df['conditions'],),
            'hi_temp_C'     : _float(df['high']['celsius']),
            'lo_temp_C'     : _float(df['low']['celsius']),
            'rel_humid'     : _float(df['avehumidity']),
            'avg_windspeed_mph' : _float(df['avewind']['mph']),
            'avg_wind_dir'      : _float(df['avewind']['degrees']),
            'max_windspeed_mph' : _float(df['maxwind']['mph']),
            'max_wind_dir'      : _float(df['maxwind']['degrees']),
            'pct_of_precip' : _float(df['pop']),
        })
    
    # publish to wx.7day_forecast
    ch.basic_publish(
        exchange='weather',
        routing_key='7day_forecast',
        properties = pika.BasicProperties(
            content_type = 'application/json'
        ),
        body=json.dumps(daily_forecast)
    )


def main():
    features = (
        "conditions",
        "hourly",
        # "forecast",
    )
    
    
    url = "http://api.wunderground.com/api/%s/%s/q/%s.json" % (
        config.wunderground.api_key,
        "/".join(features),
        config.wunderground.station
    )
    
    connection = None
    
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=config.message_broker.host)
        )
        channel = connection.channel()
        
        try:
            # f = urllib2.urlopen(url)
            f = open("wunderground_cache.json", "r")
            parsed_json = json.loads(f.read())
        finally:
            if f != None:
                f.close()
        
        publish_hourly_forecast(parsed_json, channel)
        publish_current_observation(parsed_json, channel)
    finally:
        if connection != None:
            connection.close()
        
    


if __name__ == '__main__':
    main()

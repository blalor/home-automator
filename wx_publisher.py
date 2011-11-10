#!/usr/bin/env python
# encoding: utf-8
"""
wx_publisher.py

Created by Brian Lalor on 2011-11-09.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys, os

import json, urllib2
import time_util

from pprint import pprint

WUNDERGROUND_KEY = 'my key here'

def fcttime_to_datetime(fcttime):
    # {u'age': '',
    #  u'ampm': u'AM',
    #  u'civil': u'8:00 AM',
    #  u'epoch': u'1320843629',
    #  u'hour': u'8',
    #  u'hour_padded': u'08',
    #  u'isdst': u'0',
    #  u'mday': u'9',
    #  u'mday_padded': u'09',
    #  u'min': u'00',
    #  u'mon': u'11',
    #  u'mon_abbrev': u'Nov',
    #  u'mon_padded': u'11',
    #  u'month_name': u'November',
    #  u'month_name_abbrev': u'Nov',
    #  u'pretty': u'8:00 AM EST on November 09, 2011',
    #  u'sec': u'29',
    #  u'tz': '',
    #  u'weekday_name': u'Wednesday',
    #  u'weekday_name_abbrev': u'Wed',
    #  u'weekday_name_night': u'Wednesday Night',
    #  u'weekday_name_night_unlang': u'Wednesday Night',
    #  u'weekday_name_unlang': u'Wednesday',
    #  u'yday': u'312',
    #  u'year': u'2011'},
    
    return time_util.dt_from_epoch_as_system_tz(int(fcttime['epoch']))
    

    



def main():
    features = ("conditions", "hourly", "forecast")
    station = "pws:kmamedfo4"
    url = "http://api.wunderground.com/api/%s/%s/q/%s.json" % (WUNDERGROUND_KEY, "/".join(features), station)
    
    if os.path.exists("wunderground_cache.json"):
        print "using cache"
        with open("wunderground_cache.json") as ifp:
            json_string = ifp.read()
        
    else:
        f = urllib2.urlopen(url)
        json_string = f.read()
        f.close()
        
        with open("wunderground_cache.json", "w") as ofp:
            ofp.write(json_string)
    
    parsed_json = json.loads(json_string)
    
    hourly_forecast = []
    for hf in parsed_json['hourly_forecast']:
        hf['FCTTIME'] = fcttime_to_datetime(hf['FCTTIME'])
        
        # {u'FCTTIME': datetime.datetime(2011, 11, 10, 10, 0, 29, tzinfo=<StaticTzInfo 'EST'>),
        #  u'condition': u'Chance of Rain',
        #  u'dewpoint': {u'english': u'56', u'metric': u'13'},
        #  u'fctcode': u'10',
        #  u'feelslike': {u'english': u'61', u'metric': u'16'},
        #  u'heatindex': {u'english': u'61',
        #                 u'metric': u'-9998'},
        #  u'humidity': u'87',
        #  u'icon': u'chancerain',
        #  u'icon_url': u'http://icons-ak.wxug.com/i/c/k/chancerain.gif',
        #  u'pop': u'83',
        #  u'qpf': {u'english': '', u'metric': u'0.00'},
        #  u'sky': u'82',
        #  u'snow': {u'english': '', u'metric': u'0.00'},
        #  u'temp': {u'english': u'61', u'metric': u'16'},
        #  u'uvi': u'1',
        #  u'wdir': {u'degrees': u'120', u'dir': u'ESE'},
        #  u'windchill': {u'english': u'61',
        #                 u'metric': u'-9998'},
        #  u'wspd': {u'english': u'5', u'metric': u'8'},
        #  u'wx': u' Chance of Light Rain Showers , Areas of Light Drizzle , Patchy Fog '},
        
        hourly_forecast.append({
            'timestamp'     : hf['FCTTIME'],
            'temp_C'        : float(hf['temp']['metric']),
            'dewpoint_C'    : float(hf['dewpoint']['metric']),
            'windchill_C'   : float(hf['windchill']['metric']),
            'rel_humid'     : float(hf['humidity']),
            'windspeed_mph' : float(hf['wspd']['english']),
            'wind_dir'      : float(hf['wdir']['degrees']),
            'pct_of_precip' : float(hf['pop'])
        })
        
    pprint(hourly_forecast)
    
    # {u'dewpoint_c': 4,
    #  u'dewpoint_f': 40,
    #  u'dewpoint_string': u'40 F (4 C)',
    #  u'display_location': {u'city': u'Medford',
    #                        u'country': u'US',
    #                        u'country_iso3166': u'US',
    #                        u'elevation': u'13.00000000',
    #                        u'full': u'Medford, MA',
    #                        u'latitude': u'42.415516',
    #                        u'longitude': u'-71.111511',
    #                        u'state': u'MA',
    #                        u'state_name': u'Massachusetts',
    #                        u'zip': u'02153'},
    #  u'estimated': {},
    #  u'forecast_url': u'http://www.wunderground.com/US/MA/Medford.html',
    #  u'heat_index_c': u'NA',
    #  u'heat_index_f': u'NA',
    #  u'heat_index_string': u'NA',
    #  u'history_url': u'http://www.wunderground.com/history/airport/KMAMEDFO4/2011/11/9/DailyHistory.html',
    #  u'icon': u'clear',
    #  u'icon_url': u'http://icons-ak.wxug.com/i/c/k/nt_clear.gif',
    #  u'image': {u'link': u'http://www.wunderground.com',
    #             u'title': u'Weather Underground',
    #             u'url': u'http://icons-ak.wxug.com/graphics/wu2/logo_130x80.png'},
    #  u'local_epoch': u'1320837313',
    #  u'local_time_rfc822': u'Wed, 09 Nov 2011 06:15:13 -0500',
    #  u'local_tz_long': u'America/New_York',
    #  u'local_tz_short': u'EST',
    #  u'ob_url': u'http://www.wunderground.com/cgi-bin/findweather/getForecast?query=42.415516,-71.111511',
    #  u'observation_epoch': u'1320837312',
    #  u'observation_location': {u'city': u'Emerson St. Across from Police Station, Medford',
    #                            u'country': u'US',
    #                            u'country_iso3166': u'US',
    #                            u'elevation': u'12 ft',
    #                            u'full': u'Emerson St. Across from Police Station, Medford, Massachusetts',
    #                            u'latitude': u'42.415516',
    #                            u'longitude': u'-71.111511',
    #                            u'state': u'Massachusetts'},
    #  u'observation_time': u'Last Updated on November 9, 6:15 AM EST',
    #  u'observation_time_rfc822': u'Wed, 09 Nov 2011 06:15:12 -0500',
    #  u'precip_1hr_in': u'0.00',
    #  u'precip_1hr_metric': u' 0',
    #  u'precip_1hr_string': u'0.00 in ( 0 mm)',
    #  u'precip_today_in': u'0.00',
    #  u'precip_today_metric': u'0',
    #  u'precip_today_string': u'0.00 in (0 mm)',
    #  u'pressure_in': u'30.17',
    #  u'pressure_mb': u'1021.6',
    #  u'pressure_trend': u'+',
    #  u'relative_humidity': u'83%',
    #  u'station_id': u'KMAMEDFO4',
    #  u'temp_c': 7.1,
    #  u'temp_f': 44.7,
    #  u'temperature_string': u'44.7 F (7.1 C)',
    #  u'visibility_km': u'9.7',
    #  u'visibility_mi': u'6.0',
    #  u'weather': u'Clear',
    #  u'wind_degrees': 260,
    #  u'wind_dir': u'West',
    #  u'wind_gust_mph': 0,
    #  u'wind_mph': 0.0,
    #  u'wind_string': u'Calm',
    #  u'windchill_c': u'7',
    #  u'windchill_f': u'45',
    #  u'windchill_string': u'45 F (7 C)'}
    
    co = parsed_json['current_observation']
    pprint({
        'timestamp'     : time_util.dt_from_epoch_with_tz(int(co['local_epoch']), co['local_tz_long']),
        'temp_C'        : co['temp_c'],
        'windchill_C'   : float(co['windchill_c']),
        'dewpoint_C'    : co['dewpoint_c'],
        'rel_humid'     : float(co['relative_humidity'].replace("%", "")),
        'windspeed_mph' : co['wind_mph'],
        'windgust_mph'  : co['wind_gust_mph'],
        'wind_dir'      : co['wind_degrees'],
        'pressure_mb'   : float(co['pressure_mb']),
        'visibility_mi' : float(co['visibility_mi']),
        'weather'       : co['weather'],
        'precip_mm'     : {
            '1hr'   : float(co['precip_1hr_metric']),
            'today' : float(co['precip_today_metric']),
        },
        'pct_of_precip' : float(hf['pop'])
    })
    


if __name__ == '__main__':
    main()

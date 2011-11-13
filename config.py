from support.attrdict import attrdict

config_data = attrdict({
    "message_broker": {
        "host" : "rabbitmq_host",
        
        # exchanges to be declared
        "exchanges" : {
            "raw_xbee_frames" : { "type" : "topic" },
            "sensor_data"     : { "type" : "topic" },
            "events"          : { "type" : "topic" },
        }
    },
    
    "wunderground" : {
        "api_key" : "my api key",
    },
    
    "xmlrpc_server" : {
        "host" : "xmlrpc_server_host",
        "port" : 9999,
    },
    
    "uploader" : {
        "url" : "http://example.com/upload_data",
        "auth" : {
            "realm" : "basic auth realm",
            "username" : "http_user",
            "password" : "http_password"
        },
        
        "temp_sensor_node_map" : {
            '00:11:22:33:44:55:66:00' : 'office.temperature',
            '00:11:22:33:44:55:66:01' : 'living_room.temperature',
            '00:11:22:33:44:55:66:02' : 'garage.temperature',
            '00:11:22:33:44:55:66:03' : 'basement.temperature'
        },
        
        "humid_sensor_node_map" : {
            '00:11:22:33:44:55:66:00': 'office.humidity',
            '00:11:22:33:44:55:66:01': 'living_room.humidity'
        }
    }
})

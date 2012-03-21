var countdown_timer = 0; // seconds
var sprinkler_active = true;
var timeout_id = null;

// socket.io connection
var conn = null;

function update_display() {
    if (sprinkler_active) {
        // show button in red, "stop" text
        $("#the_button").attr("class", "button_stop");
        $("#the_button > p").text("stop");
        $("#countdown").text("on");
    } else {
        // show button in green, "start" text
        $("#the_button").attr("class", "button_start");
        $("#the_button > p").text("start");
        $("#countdown").text("off");
    }

}

function activate_sprinkler(id) {
    RabbitRPC.invoke_rpc_method(
        'sprinkler', 'activate_sprinkler', [id],
        function(success) {
            sprinkler_active = success;

            if (success) {
                $("#footer").text("sprinkler activated");
            } else {
                $("#footer").text("sprinkler failed to activate");
            }

            update_display();
        }
    );
    
    update_display();
}

function deactivate_sprinkler(id) {
    sprinkler_active = false;
    
    // cancel the running timeout, if necessary
    if (timeout_id != null) {
        window.clearTimeout(timeout_id);
        
        timeout_id = null;
    }

    RabbitRPC.invoke_rpc_method(
        'sprinkler', 'deactivate_sprinkler', [id],
        function(success) {
            sprinkler_active = (! success);

            if (success) {
                $("#footer").text("sprinkler deactivated");
            } else {
                $("#footer").text("couldn't stop sprinkler!");
            }
        
            // update_countdown(0);
            update_display();
        }
    );
}

function handle_button_click() {
    if (sprinkler_active) {
        deactivate_sprinkler(1);
    } else {
        activate_sprinkler(1);
    }
    
    update_display();
}

function update_countdown(duration) {
    var repeat = true;
    
    if (timeout_id != null) {
        window.clearTimeout(timeout_id);
        timeout_id = null;
    }
    
    if (duration != undefined) {
        countdown_timer = duration;
    } else {
        countdown_timer -= 1;
    }
    
    var countdown_text = "off";
    
    if (countdown_timer > 0) {
        countdown_text =
            pad(Math.floor(countdown_timer / 60), 1) +
            ":" +
            pad((countdown_timer % 60), 2);
    } else {
        repeat = false;
    }
    
    $("#countdown").text(countdown_text);
    
    if (repeat) {
        timeout_id = window.setTimeout(update_countdown, 1000);
    } else {
        sprinkler_active = false;
    }
    
    update_display();
}

function poll_time_remaining() {
    RabbitRPC.invoke_rpc_method(
        'furnace', 'get_time_remaining', null,
        function(seconds_remaining) {
            update_countdown(seconds_remaining);
        }
    );
}

function handle_sensor_data(msg) {
    // {
    //     "deliveryInfo": {
    //         "consumerTag": "node-amqp-142-0.6827158806845546", 
    //         "contentType": "application/json", 
    //         "deliveryTag": 21, 
    //         "exchange": "sensor_data", 
    //         "queue": "amq.gen-pt9b4ORHWEijI6hKN93vPg==", 
    //         "redelivered": false, 
    //         "routingKey": "furnace"
    //     }, 
    //     "headers": {}, 
    //     "body": { 
    //         "time_remaining": 0,
    //         "timestamp": '2011-11-21T14:46:49.289614+00:00',
    //         "zone_active": 0,
    //         "zone_state": 'inactive',
    //         "powered": true
    //     }
    // }
    
    var message = msg.body;
    
    if (msg.deliveryInfo.routingKey == 'furnace') {
        if (message.time_remaining > 0) {
            update_countdown(message.time_remaining);
        } else {
            if (message.zone_active == 1) {
                $("#countdown").text("running");
            } else {
                $("#countdown").text("off");
            }
        }
        
        var pwr_txt = null;
        if (message.powered) {
            pwr_txt = "powered";
        } else {
            pwr_txt = "not powered"
        }
        
        $("#footer").text(sprintf(
            "zone %s, %s, %d remaining",
            message.zone_state,
            pwr_txt,
            message.time_remaining
        ));
    }
}

function do_on_load() {
    // wire the button to the click handler
    $("#the_button").click(handle_button_click);
    
    // connect to the server
    conn = io.connect();
    
    RabbitRPC.prepare_connection(conn);
    
    // wire up the event handlers
    // conn.on('sensor_data', handle_sensor_data);
    
    conn.on('error', function(e) {
        console.log("got error in socket.io connection");
        console.log(e);
        console.dir(arguments);
    });
    
    // need to have the client not send any subscribe requests until the
    // queue is created
    conn.on('ready', function(msg) {
        console.log("connection ready");
        
        // // subscribe to the topics we're interested in
        // conn.emit('subscribe', {exchange: 'sensor_data', topics: ['furnace']});
        
        // poll_time_remaining();

        update_display();
    });
}

$(document).ready(do_on_load);

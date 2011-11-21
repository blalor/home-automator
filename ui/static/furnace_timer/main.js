var countdown_timer = 0; // seconds
var timer_active = false;
var timeout_id = null;

// socket.io connection
var conn = null;

var pending_rpc_requests = {};

function update_button() {
    if (timer_active) {
        // show button in red, "stop" text
        $("#the_button").attr("class", "button_stop");
        $("#the_button > p").text("stop");
    } else {
        // show button in green, "start" text
        $("#the_button").attr("class", "button_start");
        $("#the_button > p").text("start");
    }
}

function start_timer() {
    timer_active = true;
    
    invoke_rpc_method('furnace', 'start_timer', null, function(started) {
        if (started) {
            $("#footer").text("timer started");
        } else {
            $("#footer").text("timer failed to start");
        }
        
        poll_time_remaining();
    });
    
    update_button();
}

function cancel_timer() {
    timer_active = false;
    
    // cancel the running timeout, if necessary
    if (timeout_id != null) {
        window.clearTimeout(timeout_id);
        
        timeout_id = null;
    }

    invoke_rpc_method('furnace', 'cancel_timer', null, function(stopped) {
        if (stopped) {
            $("#footer").text("timer stopped");
        } else {
            $("#footer").text("timer failed to stop!");
        }
        
        update_countdown(0);
    });
}

function handle_button_click() {
    if (timer_active) {
        cancel_timer();
    } else {
        start_timer();
    }
    
    update_button();
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
        timer_active = false;
    }
    
    update_button();
}

function handle_rpc_reply(msg) {
    var response = msg.reply;
    var ticket = msg.ticket;
    
    var cb = null;
    
    // retrieve and invoke callback, if provided
    if (pending_rpc_requests.hasOwnProperty(ticket)) {
        cb = pending_rpc_requests[ticket];
        
        delete pending_rpc_requests[ticket];
    }
    
    if (response.hasOwnProperty('exception')) {
        throw response.exception;
    }
    
    if (cb != null) {
        cb(response.result);
    }
}

function invoke_rpc_method(queue, command, args, cb) {
    conn.emit(
        'rpc_request',
        {
            queue: queue,
            command: command,
            args: args
        },
        function(ticket) {
            if (typeof cb == 'function') {
                pending_rpc_requests[ticket] = cb;
            }
        }
    );
}

function poll_time_remaining() {
    invoke_rpc_method('furnace', 'get_time_remaining', null, function(seconds_remaining) {
        update_countdown(seconds_remaining);
    });
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
    
    // wire up the event handlers
    conn.on('sensor_data', handle_sensor_data);
    conn.on('rpc_reply', handle_rpc_reply);
    
    conn.on('error', function(e) {
        console.log("got error in socket.io connection");
        console.log(e);
        console.dir(arguments);
    });
    
    // need to have the client not send any subscribe requests until the
    // queue is created
    conn.on('ready', function(msg) {
        console.log("connection ready");
        
        // subscribe to the topics we're interested in
        conn.emit('subscribe', {exchange: 'sensor_data', topics: ['furnace']});
        
        poll_time_remaining();
    });
}

$(document).ready(do_on_load);

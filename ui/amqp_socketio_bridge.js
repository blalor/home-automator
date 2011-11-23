/**
    Bridge between AMQP (RabbitMQ) server and Socket.IO clients.
    
    Sample template below:

    var pending_rpc_requests = {};
    
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

    // connect to the server
    conn = io.connect('http://localhost:8080');

    // wire up the event handlers
    
    // RPC replies get their own event
    conn.on('rpc_reply', handle_rpc_reply);
    
    // event name is the exchange the message is coming from
    conn.on('sensor_data', function(msg) { … });
    
    // need to have the client not send any subscribe requests until the
    // queue is created
    conn.on('ready', function(msg) {
        // subscribe to the topics we're interested in
        conn.emit('subscribe', {exchange: 'sensor_data', topics: ['furnace']});
    });
**/

var log4js = require('log4js');
log4js.configure('log4js.json', {reloadSecs : 5});

var logger = log4js.getLogger();
var amqp_logger = log4js.getLogger('amqp');
var http_logger = log4js.getLogger('http');

var amqp = require('amqp');
var socket_io = require('socket.io');
var uuid = require('node-uuid');

var http = require("http");
var url = require("url");
var path = require("path");
var fs = require("fs");

var static = require('node-static');

// override/provide additional mime types
var staticMimeTypes = require('node-static/lib/node-static/mime');

staticMimeTypes.contentTypes['xhtml'] = 'application/xhtml+xml';
staticMimeTypes.contentTypes['html']  = 'application/xhtml+xml';
staticMimeTypes.contentTypes['eot']   = 'application/vnd.ms-fontobject';
staticMimeTypes.contentTypes['ttf']   = 'application/x-font-ttf';
staticMimeTypes.contentTypes['woff']  = 'application/x-font-woff';

var staticServer = new (static.Server)('./static', { cache: 10 });

var amqp_conn = amqp.createConnection({ host: 'pepe.home.bravo5.org' });

// Wait for connection to become established.
amqp_conn.on("ready", function () {
    amqp_logger.info("connection made");
});

amqp_conn.on("error", function () {
    amqp_logger.error("connection error");
});

var http_server = http.createServer(function(request, response) {
    request.addListener('end', function () {
        // Serve files
        staticServer.serve(request, response, function (err, res) {
            if (err) { // An error as occured
                http_logger.error("Error serving " + request.url + " - " + err.message);
                response.writeHead(err.status, err.headers);
                response.end();
            } else { // The file was served successfully
                http_logger.debug(request.url + " - " + res.message);
            }
        });
    });
});

// just because I can…
http_server.on('connection', function(socket) {
    var remoteAddr = socket.remoteAddress;
    
    http_logger.debug("connection from " + remoteAddr);
    
    socket.on('close', function(had_error) {
        if (had_error) {
            http_logger.error("connection from " + remoteAddr + " closed with error");
        } else {
            http_logger.debug("connection from " + remoteAddr + " closed");
        }
    });
});

var io = socket_io.listen(
    http_server,
    {
        logger : log4js.getLogger('socket.io'),
        'log level' : log4js.levels.INFO
    }
);

io.sockets.on('connection', function (socket) {
    logger.info("client connected");
    
    // Create a queue of our very own
    var q = amqp_conn.queue('', {exclusive: true}, function(the_queue) {
        amqp_logger.trace("queue subscribed");
        
        // dispatch received messages (or RPC replies) to the client
        // queue is not bound, yet
        q.subscribe(function (message, headers, deliveryInfo) {
            
            amqp_logger.trace([message, headers, deliveryInfo]);
            
            // if the content-type is application/json, no parsing is needed
            
            if (deliveryInfo.correlationId) {
                // this is an RPC reply
                amqp_logger.trace("rpc reply for " + deliveryInfo.correlationId, message);
                
                // message : { result: { foo: 'bar' } },
                // headers : {},
                // deliveryInfo : {
                //     contentType: 'application/json',
                //     correlationId: 'ee24fb73-957a-4675-8c90-02be129abe9e',
                //     queue: '…',
                //     deliveryTag: 1,
                //     redelivered: false,
                //     exchange: '',
                //     routingKey: '…',
                //     consumerTag: '…'
                // }
                
                socket.emit(
                    'rpc_reply',
                    {
                        ticket : deliveryInfo.correlationId,
                        reply  : message
                    }
                )
            }
            else {
                amqp_logger.trace("got normal message", message);
                
                // message : {
                //     timestamp: '2011-11-20T14:54:30.177785+00:00',
                //     zone_active: 0
                // },
                // headers : {},
                // deliveryInfo : {
                //     contentType: 'application/json',
                //     queue: '…',
                //     deliveryTag: 2,
                //     redelivered: false,
                //     exchange: 'sensor_data',
                //     routingKey: 'furnace',
                //     consumerTag: '…'
                // }
                    
                socket.emit(
                    deliveryInfo.exchange,
                    {
                        body         : message,
                        headers      : headers,
                        deliveryInfo : deliveryInfo
                    }
                );
            }
        });
        
        // need to have the client not send any subscribe requests until the
        // queue is created
        socket.emit('ready');
    });
    
    q.on("error", function (e) {
        amqp_logger.error("queue error", e);
    });
    
    q.on("close", function () {
        amqp_logger.warn("queue closed");
    });
    
    q.on("open", function () {
        amqp_logger.info("queue opened");
    });
    
    // store it for later retrieval
    socket.set('queue', q);
    
    // handles the "subscribe" event, emitted by a client who wants to receive
    // one or more topics from an exchange.  
    socket.on('subscribe', function(data) {
        // {
        //     exchange : "exchange",
        //     topics : ["topic1", "topic2"]
        // }
        
        socket.get('queue', function(err, q) {
            for (var t_ind = 0; t_ind < data.topics.length; t_ind++) {
                var topic = data.topics[t_ind];
                
                amqp_logger.debug("binding to " + data.exchange + " " + topic);
                q.bind(data.exchange, topic);
            }
        });
    });
    
    // handles the "rpc_request" event, emitted by a client invoking an RPC 
    // method. Client expects a reply, which is the request ticket (correlation 
    // id).  Ticket is transmitted with the reply via the queue's subscribe
    // handler
    socket.on('rpc_request', function(rpc_req_msg, ack) {
        // {
        //     args : null | obj | array,
        //     command : ,
        //     queue :
        // }
        
        socket.get('queue', function(err, q) {
            var ticket = uuid();
            
            amqp_logger.trace('got rpc_request', {rpc_req_msg:rpc_req_msg, ack:ack, ticket:ticket});
            
            // make sure args is an array of values
            args = [];
            if (
                rpc_req_msg.hasOwnProperty('args') &&
                (rpc_req_msg.args != null)
            ) {
                if (rpc_req_msg.args instanceof Array) {
                    args = rpc_req_msg.args;
                } else {
                    args = [rpc_req_msg.args];
                }
            }
            
            amqp_conn.publish(
                rpc_req_msg.queue, // recipient queue
                {                  // message body
                    command : rpc_req_msg.command,
                    args    : args
                },
                {                  // message options
                    mandatory     : true,
                    immediate     : true,
                    replyTo       : q.name,
                    correlationId : ticket
                }
            );
            
            ack(ticket);
        });
    });
    
    socket.on('disconnect', function(arg) {
        logger.info('client disconnected');
        
        socket.get('queue', function(err, q) {
            amqp_logger.debug("destroying queue");
            
            try {
                q.destroy();
            } catch (ex) {
                amqp_logger.error("exception destroying queue", ex);
            }
        });
    });
});

logger.info("listening");
http_server.listen(8000);

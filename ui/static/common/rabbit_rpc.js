var RabbitRPC = RabbitRPC || {};

RabbitRPC = {
    private : {
        conn : null,
        pending_rpc_requests : {},
        handle_rpc_reply : function (msg) {
            var response = msg.reply;
            var ticket = msg.ticket;

            var cb = null;

            // retrieve and invoke callback, if provided
            if (RabbitRPC.private.pending_rpc_requests.hasOwnProperty(ticket)) {
                cb = RabbitRPC.private.pending_rpc_requests[ticket];

                delete RabbitRPC.private.pending_rpc_requests[ticket];
            }

            if (response.hasOwnProperty('exception')) {
                throw response.exception;
            }

            if (cb != null) {
                cb(response.result);
            }
        }
    },
    prepare_connection: function(conn) {
        // conn is a socket.io connection
        this.private.conn = conn;
        this.private.conn.on('rpc_reply', this.private.handle_rpc_reply);
    },
    invoke_rpc_method: function(queue, command, args, cb) {
        this.private.conn.emit(
            'rpc_request',
            {
                queue: queue,
                command: command,
                args: args
            },
            function(ticket) {
                if (typeof cb == 'function') {
                    RabbitRPC.private.pending_rpc_requests[ticket] = cb;
                }
            }
        );
    }
    
};

// socket.io connection
var conn = null;

function display_node_details(node_details) {
    
    node_details.last_frame = new Date(node_details.last_frame).toLocaleString();
    
    var node_details_row_id = "node-" + node_details.node_id.replace(/:/g, "_");
    
    console.log(node_details_row_id);
    
    var node_row = $("#" + node_details_row_id);
    
    // create the row if one does not exist
    if (node_row.length == 0) {
        // create row
        node_row = $("<tr />")
            .attr("id", node_details_row_id);
        
        $("#devices").append(node_row);
        
        var params = [
            "node_id",
            "ident",
            "node_type",
            "version",
            "frame_count",
            "last_frame",
            "stale"
        ];
        
        for (var p = 0; p < params.length; p++) {
            node_row.append(
                $("<td>").attr("class", params[p])
            );
        }
    }
    
    // using the table cell's class as a key, populate the cell contents with
    // the appropriate property
    $("td", node_row).each(function(ind, elt) {
        $(elt).text(
            node_details[$(elt).attr("class")]
        );
    });
}

function update_nodes(node_list) {
    // console.log("node_list, " + node_list.length);
    
    for (var i = 0; i < node_list.length; i++) {
        RabbitRPC.invoke_rpc_method(
            'zb_net_monitor',
            'get_node_details',
            node_list[i],
            display_node_details
        );

        // console.log(node_list[i]);
    }
}

function handle_event_message(msg) {
    if (msg.deliveryInfo.routingKey == 'xbee_network_monitor') {
        if (msg.body.name == 'discovery_complete') {
            // refresh all data
            RabbitRPC.invoke_rpc_method(
                'zb_net_monitor',
                'get_discovered_nodes',
                null,
                update_nodes
            );
        } else if (msg.body.name == 'new_node') {
            var node_id = msg.body.data.node_id;
            
            update_nodes([node_id]);
        }
    }
}

function do_on_load() {
    // connect to the server
    conn = io.connect();
    
    RabbitRPC.prepare_connection(conn);
    
    // wire up the event handlers
    conn.on('events', handle_event_message);
    
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
        conn.emit('subscribe', {exchange: 'events', topics: ['xbee_network_monitor']});
        
        RabbitRPC.invoke_rpc_method(
            'zb_net_monitor',
            'get_discovered_nodes',
            null,
            update_nodes
        );
        
    });
}

$(document).ready(do_on_load);

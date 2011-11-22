var conn = null;
var chart = null;
var timeout = null;

var power_timestamps = [];
var furnace_timestamps = [];

function dispatch_sensor_data(msg) {
    if (msg.deliveryInfo.routingKey == 'electric_meter') {
        handle_electric_meter(msg);
    } else if (msg.deliveryInfo.routingKey == 'furnace') {
        handle_furnace(msg);
    }
}

function handle_furnace(msg) {
    var sample = msg.body;
    sample.timestamp = new Date(sample.timestamp);

    var do_shift = false;
    
    furnace_timestamps.push(sample.timestamp);
    var first_ts = furnace_timestamps[0];
    var last_ts = furnace_timestamps[furnace_timestamps.length - 1];
    
    if ((last_ts - first_ts) > 900000) {
        // 15 minutes
        furnace_timestamps.shift();
        do_shift = true;
    }
    
    var point_val = 'on';
    if (sample.zone_active == 0) {
        point_val = 'off';
    }
    
    var point = [
        sample.timestamp.getTime() - (sample.timestamp.getTimezoneOffset() * 60000),
        point_val
    ];

    point.push();

    // point, redraw, shift, animation
    chart.series[0].addPoint(point, true, do_shift, false);
    
    chart.redraw();
}

function handle_electric_meter(msg) {
    // body : {
    //     timestamp: '2011-11-21T22:44:56.321502+00:00',
    //     clamp2_amps: 2.04,
    //     clamp1_amps: 0.46
    // },
    // headers: {},
    // deliveryInfo : {
    //     contentType: 'application/json',
    //     queue: 'amq.gen-JsoziZleVh3yp5xAoMRCbw==',
    //     deliveryTag: 1,
    //     redelivered: false,
    //     exchange: 'sensor_data',
    //     routingKey: 'electric_meter',
    //     consumerTag: 'node-amqp-17675-0.7634610722307116'
    // }
    
    var sample = msg.body;
    sample.timestamp = new Date(sample.timestamp);

    var do_shift = false;
    
    power_timestamps.push(sample.timestamp);
    var first_ts = power_timestamps[0];
    var last_ts = power_timestamps[power_timestamps.length - 1];
    
    if ((last_ts - first_ts) > 900000) {
        // 15 minutes
        power_timestamps.shift();
        do_shift = true;
    }

    var point = [
        sample.timestamp.getTime() - (sample.timestamp.getTimezoneOffset() * 60000),
        sample.clamp1_amps + sample.clamp2_amps
    ];

    point.push();

    // point, redraw, shift, animation
    chart.series[1].addPoint(point, true, do_shift, false);
    
    chart.redraw();
};

$(document).ready(function() {
    chart = new Highcharts.Chart({
        chart: {
            renderTo: 'chart',
            type: 'line',
            alignTicks: false,
            borderRadius: 0
        },
        credits : {
            enabled : false
        },
        title: {
            text: ''
        },
        legend: {
            enabled: false
        },
        xAxis: {
            type: 'datetime'
            // maxZoom: 14 * 24 * 3600000 // fourteen days
            // title: {
            //  text: null
            // }
        },
        yAxis: [
            { // primary
                title: {
                    text: ''
                }
            },
            { // secondary
                title: {
                    text: ''
                },
                opposite: true,
                categories: ['off', 'on']
            }
        ],
        tooltip: {
            shared: true,
            crosshairs: true
        },
        series: [
            {
                name: 'furnace',
                type: 'area',
                yAxis: 1,
                data: [],
                marker: {
                    enabled: false
                }
            },
            {
                name: 'power',
                type : 'area',
                yAxis: 0,
                color : 'rgb(122,214,122)',
                data: [],
                marker : {
                    enabled: false
                }
            }
        ]
    });
    
    // connect to the server
    conn = io.connect();
    
    // wire up the event handlers
    conn.on('sensor_data', dispatch_sensor_data);
    
    conn.on('ready', function(msg) {
        console.log("connection ready");
        
        // subscribe to the topics we're interested in
        conn.emit('subscribe', {
            exchange: 'sensor_data', 
            topics: ['electric_meter', 'furnace']
        });
    });
});

Home Automator
==============

Home monitoring and (fledgling) automation framework built on the [RabbitMQ][1] message broker.

Overview
--------

I'm slowly making my house "smart".  Currently active are:

* temperature and humidity sensors throughout the house
* a power monitor attached to the electric meter
• an antique voltmeter reflecting real-time power consumption
* a remotely-operated sprinkler controller for my lawn
* an occasionally-working device in my fuel oil tank to measure volume
* a furnace activity monitor and timer-based override

All of this data needs to go somewhere, and often several somewheres.  Long-term data collection, event response, real-time analysis and graphing, and behavior modification (both for me and the house) are some of the goals I have for this system.  It's been a work in progress over several years, frequently interrupted as I context-switch between this and other electronics, car, home, work and personal projects.

A few months ago I heard about [RabbitMQ][1], started learning a bit more about message brokers, and realized that [pub/sub][2] was the ideal model for this system: multiple sources of information being presented to multiple consumers at once.  I'd known for a while that I was reinventing the wheel by implementing my own — not terribly efficient — [hub-and-spoke system][3] for getting information from a sensor to a consumer, but it finally dawned on me that if I wanted to grow the system I needed a better backbone.

Currently, most of the devices have [Arduino-based][4] microcontrollers that communicate wirelessly with a central gateway using [XBee modules][5].

![system layout](https://github.com/blalor/home-automator/wiki/images/system%20layout.png "system layout")

Components
----------

### `xbee_gateway.py`

Receives XBee frames and publishes them to the `raw_xbee_frames` exchange.  Also consumes messages from the `xbee_tx` queue to be transmitted as frames to remote XBee devices.

### Sensor data handlers

The following scripts consume device-specific frames from the `raw_xbee_frames` exchange and publish usable to the `sensor_data` exchange.  They are essentially device drivers.

* `environmental_node_consumer.py`
* `fuel_oil_tank_consumer.py`
* `furnace_consumer.py`
* `power_consumer.py`
* `xbee_lt_sensor.py`

The `furnace_consumer.py` script also exposes some methods via RPC to control an override timer so that we can get enough hot water for a shower.  It's a long story…

### `voltometer_driver.py`

Consumes power monitor data published by `power_consumer.py` on the `sensor_data` exchange and transmits frames (via the `xbee_tx` queue) to an XBee/microcontroller device housed in an antique voltmeter (aka the "volt-o-meter") in the living room that reflects real-time power consumption.

### `wx_publisher.py`

Invoked hourly to retrieve weather forecasts and current conditions, published to the `weather` exchange.  Not really used at this time, but I'm hoping to be able stay ahead of the heating and cooling needs of the house by reacting to the weather.

### Sensor data logger

* `db_logging_receiver.py` stores data published on the `sensor_data` exhange to an sqlite3 database
* `upload_data.py` periodically uploads the data to a more powerful server for graphing and longer-term trend analysis

### `xmlrpc_server.py`

[XML-RPC][6] bridge to the messaging-based RPC mechanism used by `furnace_consumer.py` and `sprinkler_driver.py`.  Python's XML-RPC library is so very handy, but I didn't like keeping track of multiple XML-RPC servers (one for each endpoint).

### Sprinkler controller

Interfaces with the remotely-operated sprinkler controller I built.  Sprinkler is (de-)activated via cron.

* `sprinkler_driver.py`
* `sprinkler_controller.py`

### `amqp_socketio_bridge.js`

This is an AMQP-to-[WebSocket][websocket] gateway written in [Node.js][nodejs].  The WebSocket part is actually an implementation detail under the covers of the very cool (and poorly documented) [Socket.IO][socketio].  There's some boilerplate code at the top of the file that shows how to hook the browser up so that you can subscribe to topics on an exchange and invoke RPC methods.

Several libraries are required:

* [socket.io][socketio] version 0.8.7
* [amqp][node-amqp] version 0.1.1
* [node-uuid][node-uuid] version 1.2.0
* [node-static][node-static] version 0.5.9
* [log4js][log4js] version 0.4.0

Just install these in the current directory with [npm][npm]

    $ npm install socket.io@0.8.7
    $ npm install amqp@0.1.1
    $ npm install node-uuid@1.2.0
    $ npm install node-static@0.5.9
    $ npm install log4js@0.4.0

Then:

    $ node amqp_socketio_bridge.js

### Furnace Timer UI

Simple HTML and jQuery iPhone web app.  Start the `amqp_socketio_bridge.js` and point your browser to [http://localhost:8000/furnace_timer/](http://localhost:8000/furnace_timer/).

<img src="https://github.com/blalor/home-automator/wiki/images/furnace%20timer%20screenshot.png" width="320" height="480" />

[1]: http://www.rabbitmq.com/
[2]: http://en.wikipedia.org/wiki/Publish/subscribe
[3]: https://github.com/blalor/home-automator/tree/b4a59caab7da5b0771b1b6abc151f1b64eedd326
[4]: http://arduino.cc/
[5]: http://www.ladyada.net/make/xbee/
[6]: http://en.wikipedia.org/wiki/XML-RPC
[nodejs]: http://nodejs.org/
[websocket]: http://en.wikipedia.org/wiki/WebSocket
[socketio]: http://socket.io/
[node-amqp]: http://github.com/postwait/node-amqp
[node-uuid]: http://github.com/broofa/node-uuid
[node-static]: http://github.com/cloudhead/node-static
[npm]: http://npmjs.org/
[log4js]: http://github.com/csausdev/log4js-node

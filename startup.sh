#!/bin/sh -ex

cd $(dirname $0)
./dispatcher.py /dev/ttyUSB0 38400
sleep 5
./environmental_node_consumer.py
./fuel_oil_tank_consumer.py
./furnace_consumer.py
./power_consumer.py
./xbee_lt_sensor.py

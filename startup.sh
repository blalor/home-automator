#!/bin/sh -e

cd $(dirname $0)
./dispatcher.py /dev/tts/0 115200 &
sleep 1
./power_consumer.py &
./furnace_consumer.py &
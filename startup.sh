#!/bin/sh -ex

cd $(dirname $0)
(while :; do ./dispatcher.py /dev/tty.usbserial-FTE4Y0J6 115200; done) &
sleep 1
(while :; do ./combined_consumer.py; done) &

#!/bin/sh

if [ ! -e sensors.db ] && [ ! -e upload.db ]; then
    sqlite3 sensors.db < sensors.schema
    sqlite3 upload.db < upload.schema
else
    echo "databases already exist!"
    exit 1
fi

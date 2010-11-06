#!/bin/sh

if [ ! -e sensors.db ] && [ ! -e upload.db ]; then
    sqlite3 sensors.db < sensors.schema
    cat sensors.schema upload.schema | sqlite3 upload.db
else
    echo "databases already exist!"
    exit 1
fi

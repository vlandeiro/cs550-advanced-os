#!/bin/sh

if [ -f config.json ]
    then python dht/dht.py dht/config.json
else
    echo "Missing configuration file."
fi

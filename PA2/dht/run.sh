#!/bin/sh

if [ -f config.json ]
    then python dht.py config.json
else
    echo "Missing configuration file."
fi

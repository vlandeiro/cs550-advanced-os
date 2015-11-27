#!/bin/bash
# Add our hostname to the riak erlang config
sed -i "s/127.0.0.1/`hostname -i`/" /etc/riak/vm.args
# Bind the various riak services to all IPs in the riak app config
sed -i "s/127.0.0.1/0.0.0.0/" /etc/riak/app.config

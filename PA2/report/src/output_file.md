# Output file

To generate the output file, we launched two nodes and ran different put, get, and del operations on node 1. Note that all the logs are printed in this output but that these logs have been deactivated for the benchmark.

## Node 1

```
ubuntu@ip-172-31-31-118:~/cs550-advanced-os/PA2/dht$ python dht.py config.json
INFO:DHTClient:Loading data in memory for the benchmark...
INFO:DHTClient:Benchmark data successfully loaded.
INFO:DHTServer:Starting DHT server.
INFO:DHTServer:DHT server listening on port 5000.
INFO:DHTClient:Starting DHT client.
$> put 123456 azertyuiop
DEBUG:DHTClient:put
DEBUG:DHTClient:local put
RET> True
$> put 7890123 wxcvbnqsdfghjklm
DEBUG:DHTClient:put
DEBUG:DHTClient:network put
RET> True
$> get 123456
DEBUG:DHTClient:get
DEBUG:DHTClient:local get
RET> azertyuiop
$> get 7890123
DEBUG:DHTClient:get
DEBUG:DHTClient:network get
RET> wxcvbnqsdfghjklm
$> del 123456
DEBUG:DHTClient:del
DEBUG:DHTClient:local rem
RET> True
$> del 7890123
DEBUG:DHTClient:del
DEBUG:DHTClient:network rem
RET> True
$> get 123456
DEBUG:DHTClient:get
DEBUG:DHTClient:local get
RET> None
$> get 7890123
DEBUG:DHTClient:get
DEBUG:DHTClient:network get
RET> None
$>
```

## Node 2

```
ubuntu@ip-172-31-31-119:~/cs550-advanced-os/PA2/dht$ python dht.py config.json
INFO:DHTClient:Loading data in memory for the benchmark...
INFO:DHTClient:Benchmark data successfully loaded.
INFO:DHTServer:Starting DHT server.
INFO:DHTServer:DHT server listening on port 5000.
INFO:DHTClient:Starting DHT client.
$> DEBUG:DHTServer:Request put 7890123 wxcvbnqsdfghjklm received.
DEBUG:DHTServer:put
DEBUG:DHTServer:Request get 7890123 received.
DEBUG:DHTServer:get
DEBUG:DHTServer:Request rem 7890123 received.
DEBUG:DHTServer:del
DEBUG:DHTServer:Request get 7890123 received.
DEBUG:DHTServer:get
```

# Output file

```
INFO:PeerServer:Starting the file server.
INFO:DHTServer:Starting the distributed indexing server.
INFO:PeerClient:Start the user interface.
$> register peer_client.py
True
$> list
peer_client.py
node.py
['peer_client.py', 'node.py']
$> lookup node.py
True
$> register *.py true
{True:21}
$> exit
None
```

Following is the output of one peer when 3 peers are connected to the indexing server.
```
$> list
Filename                      Size      Path
--------------------------------------------------------------------------------
2K12                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K12
2K11                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K11
4K21                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K21
4K22                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K22
2K31                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K31
2K32                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K32
1K12                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K12
1K11                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K11
1K32                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K32
16K22                         16.0KB    /home/ubuntu/cs ... p/files/u/16K22
16K21                         16.0KB    /home/ubuntu/cs ... p/files/u/16K21
8K32                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K32
8K31                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K31
8K11                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K11
8K12                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K12
4K32                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K32
1K31                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K31
4K31                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K31
1K22                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K22
2K22                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K22
2K21                          2.0KB     /home/ubuntu/cs ... 2p/files/u/2K21
1K21                          1.0KB     /home/ubuntu/cs ... 2p/files/u/1K21
16K12                         16.0KB    /home/ubuntu/cs ... p/files/u/16K12
16K11                         16.0KB    /home/ubuntu/cs ... p/files/u/16K11
8K21                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K21
8K22                          8.0KB     /home/ubuntu/cs ... 2p/files/u/8K22
4K11                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K11
4K12                          4.0KB     /home/ubuntu/cs ... 2p/files/u/4K12
16K31                         16.0KB    /home/ubuntu/cs ... p/files/u/16K31
16K32                         16.0KB    /home/ubuntu/cs ... p/files/u/16K32
$> search 16K31
File available at the following peers:
        - 52.26.121.53:5000
$> lookup 1K22
Select amongst these peers [1]
[1] 52.89.142.84:5000
Choice: 1
Download 1K22 of size 1.0KB? [Y/n] Y
Downloading file... 100%
File received and stored locally.
$> echo this is a test
IS> this is a test
$> help
benchmark           Benchmark a function (lookup, search, or register) by
                    running this command N times and averaging the runtime.
echo                Simple function that send a message to the server, wait for
                    the same message and print it.
exit                Shut down this peer.
getid               Return the peer id.
help                Display the help screen.
list                List all the available files in the indexing server.
lookup              Request the indexing server for the lists of other peers
                    that have a given file and give the choice to the user to
                    download the file from a peer.

register            Register files to the indexing server.
search              Only request the indexing server for the lists of other
                    peers having that file
$> getid
139706962797056
$> benchmark search 20
Total time: 3.04s Average time: 152.00ms
$> exit
Closing connection to indexing server.
ubuntu@ip-172-31-14-198:~/cs550-advanced-os/PA1/p2p$ ls -l files/d/
-rwxrwxr-x 1 ubuntu ubuntu 1024 Sep 21 22:38 files/d/1K22
```

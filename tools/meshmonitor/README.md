Overview
---------------
Mesh Monitor is a tool that simulates the VoltDB heartbeat paths. It is
useful in diagnosing issues such as network delays and instability, mysterious
timeouts and hangs and scheduling problems that delay message passing. A
common use for this tool is when sites are experiencing dead host timeouts
without any obvious network event.

Meshmonitor has threads that measure and report on 3 parameters:

1. **Sending heartbeats.** A *send* thread wakes up every 5 milliseconds and
sends heartbeats to all the other servers running meshmonitor. This tracks
the liveness of the server (i.e. its ability of a thread to get scheduled
in a timely manner and send a message out.)
2. **Receiving heartbeats.** A *receive* thread that is blocked reading the
socket that receives messages sent from the other servers.
3. **Timestamp differences**.The *receive* thread also
measures the difference in time between the timestamp encoded in the
heartbeat and when the heartbeat was processed.

If any of these values exceeds the hiccuptime (default of 20 milliseconds),
a message is printed out.  Otherwise, the meshmonitor prints a message
"nothing to report".

Log information
---------------
There are 3 kinds of messages, for the 3 measurements:

* delta send - delta between sending heartbeats
* delta receive - delta between receiving heartbeats
* delta timestamp - delta between remotely recorded sent timestamp and
  locally recorded receive time

**Log message fields (in order):**

*Identifying information:*  
Date  
Time  
This node address  
Other node address (i.e. message sender)  
Message type  

*Latency Information:*  
Max latency  
Average Latency  
99.0    percentile latency  
99.9    percentile latency  
99.99   percentile latency  
99.999  percentile latency  

Example:  
`2014-11-07 15:53:49,019	/10.10.181.102:41924   volt14a/10.10.181.19:12222  delta receive - MaxLat:   96 Avg:   5.58 99th-Pct:   9  86  96  96`


Interpreting Results
---------------------
Log files from all the nodes should be compared in order to establish where
the problem lies. There can be delays in many parts of the system. 
By comparing log files from different nodes you can often match deltas in
send times on one node to deltas in receive times on the others. This
can indicate that a sender is not properly scheduling its threads.  Deltas
in receive times with no correlated deltas in send times can indicate a
bottleneck in the network.

Starting Meshmonitor
--------------------
Copy meshmonitor.jar to all the nodes you want to run on.

meshmonitorhelper.sh is used to help facilitate executing mesh monitor
program.  Log will be created with the 'hostname-mesh.log' structure.  This
script will help generate the commands needed to run on all other nodes for
testing.  Please ensure network port chosen is free on all nodes.

```
Usage: ./meshmonitorhelper.sh NODESFILE <HICCUPSIZE> <LOGINTERVAL> <NETWORKPORT>
   NODESFILE - required parameter         - file with list of nodes on each line
   <HICCUPSIZE>  - optional               - mininum latency in milliseconds to report, default value = 20
   <LOGINTERVAL> - optional               - interval of logging in seconds, default value = 10
   <NETWORKPORT> - optional               - network port used, default value = 12222

```

Sample output for a nodes.txt that lists 3 nodes:  
prod1  
prod2  
client1  

```
> ./meshmonitorhelper.sh nodes.txt
Using list of hosts file: nodes.txt
Using default minimum hiccup size: 20
Using default logging interval in seconds: 10
Using default network port: 12222

Generating the commands needed to run on all other nodes:

#prod1: In <VOLTDB_HOME>/tools/meshmonitor directory, run the following command:
nohup java -jar meshmonitor.jar 20 10 prod1:12222 > prod1-mesh.log &

#prod2: In <VOLTDB_HOME>/tools/meshmonitor directory, run the following command:
nohup java -jar meshmonitor.jar 20 10 prod2:12222 prod1:12222 > prod2-mesh.log &

#client1: In <VOLTDB_HOME>/tools/meshmonitor directory, run the following command:
nohup java -jar meshmonitor.jar 20 10 client1:12222 prod2:12222 prod1:12222 > client1-mesh.log &
```

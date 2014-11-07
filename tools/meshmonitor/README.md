Mesh Monitor is a tool that simulates the VoltDB heartbeat paths. It is useful diagnosing
issues such as network delays and instability, mysterious timeouts and hangs, scheduling
problems that delay message passing. A common use for this tool is when sites are
experiencing dead host timeouts without any obvious network event.

The app won't log anything detailed if the latency between heartbeats is < the minimum
hiccup size, but it will say there is nothing to report. If there is enough latency it
will dump histograms for all things that were measured that had a hiccup. There is a
thread that wakes up every five milliseconds to send a heartbeat, and how many
milliseconds it takes to wake up is measured. This tracks the liveness of the node sending
the heartbeat. There is another thread blocked reading the socket that tracks how often it
receives a heartbeat. It also tracks the difference in time between the time encoded in
the heartbeat and when the heartbeat was processed. This is enough information to
evaluation the liveness of the nodes as well as how prompt the network is being.

Mesh Monitor helper is used to help facilitate executing mesh monitor program.
Log will be created with the 'hostname-mesh.log' structure.
This script will help generate the commands needed to run on all other nodes for testing.
Please ensure network port chosen is free on all nodes. All times are in UTC.

Usage: ./meshmonitorhelper.sh <LISTOFNODES> <HICCUPSIZE> <LOGINTERVAL> <NETWORKPORT>
   <LISTOFNODES> - required parameter	- file with list of nodes on each line
   <HICCUPSIZE>  - optional		- mininum latency in milliseconds to report, default value = 20
   <LOGINTERVAL> - optional 	- interval of logging in seconds, default value = 10
   <NETWORKPORT> - optional		- network port used, default value = 12222 [ "$2" -gt "0" ]

Sample output
# ./meshmonitorhelper.sh samplefile
Using list of hosts file: samplefile
Using default minimum hiccup size: 20
Using default logging interval in seconds: 10
Using default network port: 12222

generate the commands needed to run on all other nodes
On hosta's meshmonitor directory, run the following command:
nohup java -jar meshmonitor/meshmonitor.jar 20 10 hosta:12222 > hosta-mesh.log &

On hostb's meshmonitor directory, run the following command:
nohup java -jar meshmonitor/meshmonitor.jar 20 10 hostb:12222 hosta:12222 > hostb-mesh.log &

On hostc's meshmonitor directory, run the following command:
nohup java -jar meshmonitor/meshmonitor.jar 20 10 hostc:12222 hostb:12222 hosta:12222 > hostc-mesh.log &

On hostd's meshmonitor directory, run the following command:
nohup java -jar meshmonitor/meshmonitor.jar 20 10 hostd:12222 hostc:12222 hostb:12222 hosta:12222 > hostd-mesh.log &

Log terminalogy
delta send - delta between sending heartbeats
delta receive - delta between receiving heartbeats
delta timestamp - delta between remotely recorded sent time and locally recorded receive time

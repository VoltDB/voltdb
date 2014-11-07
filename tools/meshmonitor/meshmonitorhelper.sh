#!/bin/bash
#meshmonitorhelper.sh is used to create lines used to run meshmonitor application on multiple nodes
#Refer to README for more information.

#declare variables; default values
LISTOFHOSTS=		# $1 required
MINHICCUPSIZE=20	# $2
LOGINTERVAL=10 		# $3
NETWORKPORT=12222	# $4
HOSTSLIST=

# display syntax and exit
syntax() {
	echo "Usage: $0 NODESFILE <HICCUPSIZE> <LOGINTERVAL> <NETWORKPORT>"
	echo "   NODESFILE - required parameter         - file with list of nodes on each line"
	echo "   <HICCUPSIZE>  - optional               - mininum latency in milliseconds to report, default value = 20"
	echo "   <LOGINTERVAL> - optional               - interval of logging in seconds, default value = 10"
	echo "   <NETWORKPORT> - optional               - network port used, default value = 12222"
	echo "Please see README for more details."
	exit 0

}

# exit program with error message
exitprogram () {
	echo >&2 "$@"
	exit 1
}

# check for numeric value
isNum (){
echo $1 | grep -E -q '^[0-9]+$'
}

# main loop

#check no parameters and help switch and exit
if [ $# -eq 0 ] || [ "$1" == "-h" ]; then
    syntax
fi

# check $1/listofNODES - required parameter
LISTOFHOSTS=$1
if [ -f $LISTOFHOSTS ];	then
	echo "Using list of hosts file: $LISTOFHOSTS"
else
	exitprogram "ERROR: file '$LISTOFHOSTS' does not exist"
fi


# check $2/HICCUPSIZE - optional parameter
# check if there is a value
if [ ! -z "$2" ]; then 
		# if its a non integer; exit
		isNum $2 || exitprogram "Input Value '$2' - ERROR - Please choose a value between 0 and 50000 ms" 
		# check for value between 0 and 50000ms
		if [ "$2" -gt "0" ] && [ "$2" -lt "50000" ]; then
			MINHICCUPSIZE=$2
			echo "Using minimum hiccup size: $MINHICCUPSIZE"
		else 
			exitprogram "minimum hiccup size - ERROR - Please choose a value between 0 and 50000 ms"
		fi
else
	echo "Using default minimum hiccup size: $MINHICCUPSIZE"
fi


# check $3/LOGINTERVAL - optional parameter
# check if there is a value

if [ ! -z "$3" ]; then 
	# if its a non integer; exit
	isNum $3 || exitprogram "Input value '$3' - ERROR - Please choose a numeric value larger then 0" # if its a non integer; exit
	# check for value larger then 0
	if [ "$2" -gt "0" ]; then
		LOGINTERVAL=$3
		echo "Using logging interval in seconds: $LOGINTERVAL"
	else 
		exitprogram "logging interval - ERROR - Please choose a numeric value larger then 0"
	fi
else
	echo "Using default logging interval in seconds: $LOGINTERVAL"
fi

# check $4/NETWORKPORT - optional parameter
# check if there is a value
if [ ! -z "$4" ]; then
	isNum $4 || exitprogram "Input Value '$4' - ERROR - Please choose an unused network port between 1-65535"
	# check for value larger then 0
	if [ "$4" -gt "0" ] && [ "$4" -lt "65535" ]; then
	 	NETWORKPORT=$4
		echo "Using network port: $NETWORKPORT"
	else
		exitprogram "network port - ERROR - Please choose an unused network port between 1-65535"
	fi
else
	echo "Using default network port: $NETWORKPORT"
fi

# print commands to execute
echo
echo "Generating the commands needed to run on all other nodes:"
echo
for f in $(cat $LISTOFHOSTS); do
	export HOSTSLIST="$f:$NETWORKPORT $HOSTSLIST"
	echo "#$f: In <VOLTDB_HOME>/tools/meshmonitor directory, run the following command:"
	echo "nohup java -jar meshmonitor.jar $MINHICCUPSIZE $LOGINTERVAL $HOSTSLIST> $f-mesh.log &"
	echo
done


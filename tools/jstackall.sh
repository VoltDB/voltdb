#!/bin/bash

# kill org.voltdb processes that aren't BenchmarkController

# list all process pids and the name of the associated executable
# look for java executables
# use ps to see the command arguments for those java pids
# find args that look like an org.voltdb class main entry point
# that aren't benchmark controller
# and kill them.

# as one line:
# ps cx -o pid -o command  | grep java | awk '{print $1}' | xargs ps -w -w -o pid -o args -p | \
#  grep " org.voltdb" | grep -v BenchmarkController | awk '{print $1}'  | xargs kill -9

# a little more error friendly when no pids are found
voltdbpid=$(jps -l | grep -v "sun.tools.jps.Jps" | grep -v "org.apache.tools.ant.launch.Launcher" | grep -v "eclipse" | grep -v "slave.jar" | awk '{print $1}')
for victim in ${voltdbpid}
do
    if [ -e "${victim}.jstack" ]; then
        ls -1 ${victim}.jstack.[0-9]* 2>/dev/null | sort -t. -k3 -n -r | awk -F\. '{O=$0; $(NF)++; OFS="."; system("mv "O" "$0)}'
        mv ${victim}.jstack ${victim}.jstack.1
    fi
    jstack -l ${victim} > ${victim}.jstack
#    jstack -l -m ${victim} > ${victim}.mixedjstack
#    jstack -F -l -m ${victim} > ${victim}.forcedjstack
done
exit 0
